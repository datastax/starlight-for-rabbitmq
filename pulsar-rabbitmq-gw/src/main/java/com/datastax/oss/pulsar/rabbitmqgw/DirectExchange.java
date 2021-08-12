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
import com.datastax.oss.pulsar.rabbitmqgw.metadata.ExchangeMetadata;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.MessageId;
import org.apache.qpid.server.model.LifetimePolicy;

public class DirectExchange extends AbstractExchange {

  public DirectExchange(String name, boolean durable, LifetimePolicy lifetimePolicy) {
    super(name, Type.direct, durable, lifetimePolicy);
  }

  @Override
  public CompletableFuture<Void> bind(
      ExchangeMetadata exchangeMetadata,
      String queue,
      String routingKey,
      GatewayConnection connection) {
    BindingSetMetadata bindings = exchangeMetadata.getBindings().get(queue);
    if (!bindings.getKeys().contains(routingKey)) {
      bindings.getKeys().add(routingKey);
      // TODO: Pulsar 2.8 has an issue with subscriptions containing a / in their name. Forge
      // another name ?
      String topic = getTopicName(connection.getNamespace(), name, routingKey).toString();
      String subscriptionName = (topic + "-" + UUID.randomUUID()).replace("/", "_");
      return connection
          .getGatewayService()
          .getPulsarAdmin()
          .topics()
          .createSubscriptionAsync(topic, subscriptionName, MessageId.latest)
          .thenAccept(
              it ->
              {
                BindingMetadata bindingMetadata = new BindingMetadata(topic, subscriptionName);
                bindingMetadata.getKeys().add(routingKey);
                bindings
                    .getSubscriptions()
                    .put(subscriptionName, bindingMetadata);
              });
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> unbind(
      ExchangeMetadata exchangeMetadata,
      String queue,
      String routingKey,
      GatewayConnection connection) {
    BindingSetMetadata bindings = exchangeMetadata.getBindings().get(queue);
    for (BindingMetadata subscription : bindings.getSubscriptions().values()) {
      if (subscription.getLastMessageId() == null && subscription.getKeys().contains(routingKey)) {
        return connection
            .getGatewayService()
            .getPulsarAdmin()
            .topics()
            .getLastMessageIdAsync(subscription.getTopic())
            .thenAccept(lastMessageId -> subscription.setLastMessageId(lastMessageId.toByteArray()));
      }
    }
    return CompletableFuture.completedFuture(null);
  }
}
