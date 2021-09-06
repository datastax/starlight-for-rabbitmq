/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.pulsar.rabbitmqgw;

import com.datastax.oss.pulsar.rabbitmqgw.metadata.BindingMetadata;
import com.datastax.oss.pulsar.rabbitmqgw.metadata.BindingSetMetadata;
import com.datastax.oss.pulsar.rabbitmqgw.metadata.VirtualHostMetadata;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.MessageId;
import org.apache.qpid.server.model.LifetimePolicy;

public class FanoutExchange extends AbstractExchange {

  public FanoutExchange(String name, boolean durable, LifetimePolicy lifetimePolicy) {
    super(name, Type.fanout, durable, lifetimePolicy);
  }

  @Override
  public CompletableFuture<Void> bind(
      VirtualHostMetadata vhost,
      String exchange,
      String queue,
      String routingKey,
      GatewayConnection connection) {
    BindingSetMetadata bindings = vhost.getExchanges().get(exchange).getBindings().get(queue);
    bindings.getKeys().add(routingKey);
    Map<String, BindingMetadata> subscriptions =
        vhost.getSubscriptions().computeIfAbsent(queue, q -> new HashMap<>());
    for (BindingMetadata bindingMetadata : subscriptions.values()) {
      if (bindingMetadata.getLastMessageId() == null
          && bindingMetadata.getExchange().equals(exchange)) {
        // There's already an active subscription
        return CompletableFuture.completedFuture(null);
      }
    }
    String topic = getTopicName(connection.getNamespace(), name, "").toString();
    String subscriptionName = (topic + "-" + UUID.randomUUID()).replace("/", "_");
    return connection
        .getGatewayService()
        .getPulsarAdmin()
        .topics()
        .createSubscriptionAsync(topic, subscriptionName, MessageId.latest)
        .thenAccept(
            it ->
                subscriptions.put(
                    subscriptionName, new BindingMetadata(exchange, topic, subscriptionName)));
  }

  @Override
  public CompletableFuture<Void> unbind(
      VirtualHostMetadata vhost,
      String exchange,
      String queue,
      String routingKey,
      GatewayConnection connection) {
    Map<String, BindingSetMetadata> bindings = vhost.getExchanges().get(exchange).getBindings();
    if (bindings.get(queue).getKeys().size() == 1) {
      // Removing the last key
      Map<String, BindingMetadata> queueSubscriptions = vhost.getSubscriptions().get(queue);
      for (BindingMetadata subscription : queueSubscriptions.values()) {
        if (subscription.getLastMessageId() == null
            && subscription.getExchange().equals(exchange)) {
          return connection
              .getGatewayService()
              .getPulsarAdmin()
              .topics()
              .getLastMessageIdAsync(subscription.getTopic())
              .thenAccept(
                  lastMessageId -> {
                    subscription.setLastMessageId(lastMessageId.toByteArray());
                    bindings.remove(queue);
                  });
        }
      }
    }
    return CompletableFuture.completedFuture(null);
  }
}
