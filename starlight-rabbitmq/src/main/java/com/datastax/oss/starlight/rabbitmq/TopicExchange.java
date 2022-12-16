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
package com.datastax.oss.starlight.rabbitmq;

import com.datastax.oss.starlight.rabbitmq.metadata.BindingMetadata;
import com.datastax.oss.starlight.rabbitmq.metadata.BindingSetMetadata;
import com.datastax.oss.starlight.rabbitmq.metadata.VirtualHostMetadata;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.qpid.server.exchange.topic.TopicNormalizer;
import org.apache.qpid.server.exchange.topic.TopicParser;
import org.apache.qpid.server.model.LifetimePolicy;

public class TopicExchange extends AbstractExchange {

  public TopicExchange(String name, boolean durable, LifetimePolicy lifetimePolicy) {
    super(name, Type.topic, durable, lifetimePolicy);
  }

  @Override
  public CompletableFuture<Void> bind(
      VirtualHostMetadata vhost,
      String exchange,
      String queue,
      String routingKey,
      GatewayConnection connection) {
    String bindingKey = TopicNormalizer.normalize(routingKey);

    BindingSetMetadata bindings = vhost.getExchanges().get(exchange).getBindings().get(queue);
    if (bindings != null && !bindings.getKeys().contains(bindingKey)) {
      bindings.getKeys().add(bindingKey);

      Map<String, BindingMetadata> subscriptions =
          vhost.getSubscriptions().computeIfAbsent(queue, q -> new HashMap<>());

      return connection
          .getGatewayService()
          .getPulsarAdmin()
          .namespaces()
          .getTopicsAsync(connection.getNamespace())
          .thenCompose(
              topics -> {
                TopicParser parser = new TopicParser();
                parser.addBinding(bindingKey, null);
                List<CompletableFuture<Void>> futures = new ArrayList<>();
                for (String topic : topics) {
                  TopicName topicName = TopicName.get(topic);
                  String[] exchangeAndKey = topicName.getLocalName().split(".__");
                  if (topicName.isPersistent()
                      && exchange.equals(exchangeAndKey[0])
                      && parser.parse(exchangeAndKey.length == 2 ? exchangeAndKey[1] : "").size()
                          > 0) {

                    boolean addedToExistingSubscription = false;
                    for (BindingMetadata bindingMetadata : subscriptions.values()) {
                      if (bindingMetadata.getExchange().equals(exchange)
                          && !bindingMetadata.getKeys().contains(bindingKey)
                          && bindingMetadata.getLastMessageId() == null) {
                        bindingMetadata.getKeys().add(bindingKey);
                        addedToExistingSubscription = true;
                        break;
                      }
                    }
                    if (!addedToExistingSubscription) {
                      String subscriptionName = (topic + "-" + UUID.randomUUID()).replace("/", "_");
                      futures.add(
                          connection
                              .getGatewayService()
                              .getPulsarAdmin()
                              .topics()
                              .createSubscriptionAsync(topic, subscriptionName, MessageId.latest)
                              .thenAccept(
                                  it -> {
                                    BindingMetadata bindingMetadata =
                                        new BindingMetadata(exchange, topic, subscriptionName);
                                    bindingMetadata.getKeys().add(routingKey);
                                    subscriptions.put(subscriptionName, bindingMetadata);
                                  }));
                    }
                  }
                }
                return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
              });
    }
    return CompletableFuture.completedFuture(null);
  }

  @Override
  public CompletableFuture<Void> unbind(
      VirtualHostMetadata vhost,
      String exchange,
      String queue,
      String routingKey,
      GatewayConnection gatewayConnection) {

    String bindingKey = TopicNormalizer.normalize(routingKey);
    if (vhost.getExchanges().get(exchange).getBindings().get(queue).getKeys().remove(bindingKey)) {
      List<CompletableFuture<Void>> futures = new ArrayList<>();
      for (BindingMetadata bindingMetadata : vhost.getSubscriptions().get(queue).values()) {
        if (exchange.equals(bindingMetadata.getExchange())
            && bindingMetadata.getKeys().remove(bindingKey)
            && bindingMetadata.getKeys().isEmpty()) {
          futures.add(
              gatewayConnection
                  .getGatewayService()
                  .getPulsarAdmin()
                  .topics()
                  .getLastMessageIdAsync(bindingMetadata.getTopic())
                  .thenAccept(
                      lastMessageId ->
                          bindingMetadata.setLastMessageId(lastMessageId.toByteArray())));
        }
      }
      return CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]));
    }
    return CompletableFuture.completedFuture(null);
  }
}
