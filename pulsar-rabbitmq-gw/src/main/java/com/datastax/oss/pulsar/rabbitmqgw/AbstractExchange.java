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

import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

import com.datastax.oss.pulsar.rabbitmqgw.metadata.BindingMetadata;
import com.datastax.oss.pulsar.rabbitmqgw.metadata.ContextMetadata;
import com.datastax.oss.pulsar.rabbitmqgw.metadata.ExchangeMetadata;
import com.datastax.oss.pulsar.rabbitmqgw.metadata.VirtualHostMetadata;
import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.qpid.server.model.LifetimePolicy;

public abstract class AbstractExchange {

  enum Type {
    direct,
    fanout,
    topic,
    headers
  }

  protected final String name;
  protected final AbstractExchange.Type type;
  protected final boolean durable;
  protected final LifetimePolicy lifetimePolicy;

  protected final Map<String, Map<String, PulsarConsumer>> bindings = new ConcurrentHashMap<>();

  protected AbstractExchange(
      String name, Type type, boolean durable, LifetimePolicy lifetimePolicy) {
    this.name = name;
    this.type = type;
    this.durable = durable;
    this.lifetimePolicy = lifetimePolicy;
  }

  public String getName() {
    return name;
  }

  public String getType() {
    return type.toString();
  }

  public boolean isDurable() {
    return durable;
  }

  public LifetimePolicy getLifetimePolicy() {
    return lifetimePolicy;
  }

  public boolean hasBindings() {
    return bindings.size() != 0;
  }

  public boolean hasBinding(String bindingKey, final Queue queue) {
    if (bindingKey == null) {
      bindingKey = "";
    }
    return bindings.containsKey(queue.getName())
        && bindings.get(queue.getName()).containsKey(bindingKey);
  }

  public CompletableFuture<ContextMetadata> addBindingMetadata(
      String queue, String routingKey, ContextMetadata context, GatewayConnection connection) {
    VirtualHostMetadata vhostMetadata = context.getVhosts().get(connection.getNamespace());
    ExchangeMetadata exchangeMetadata = vhostMetadata.getExchanges().get(name);
    exchangeMetadata.getBindings().putIfAbsent(queue, new HashMap<>());
    Map<String, BindingMetadata> bindings = exchangeMetadata.getBindings().get(queue);
    if (!bindings.containsKey(routingKey)) {
      // TODO: Pulsar 2.8 has an issue with subscriptions containing a / in their name. Forge
      // another
      //  name ?
      String topic = getTopicName(connection.getNamespace(), name, routingKey).toString();
      String subscriptionName = (topic + "-" + UUID.randomUUID()).replace("/", "_");
      return connection
          .getGatewayService()
          .getPulsarAdmin()
          .topics()
          .createSubscriptionAsync(topic, subscriptionName, MessageId.latest)
          .thenApply(
              it -> {
                bindings.put(routingKey, new BindingMetadata(topic, subscriptionName));
                return context;
              });
    }
    return CompletableFuture.completedFuture(context);
  }

  public CompletableFuture<ContextMetadata> bind(
      String queue, String routingKey, GatewayConnection connection) {
    ContextMetadata newContext = connection.getGatewayService().getAmqContext().toMetadata();
    return addBindingMetadata(queue, routingKey, newContext, connection)
        .thenCompose(
            contextMetadata -> connection.getGatewayService().saveContext(contextMetadata));
  }

  public CompletableFuture<Void> unbind(Queue queue, String routingKey) {
    String queueName = queue.getName();
    PulsarConsumer pulsarConsumer = null;
    if (bindings.containsKey(queueName)) {
      Map<String, PulsarConsumer> binding = bindings.get(queueName);
      if (binding.containsKey(routingKey)) {
        pulsarConsumer = binding.get(routingKey);
        binding.remove(routingKey);
      }
      if (binding.size() == 0) {
        bindings.remove(queueName);
        queue.getBoundExchanges().remove(this);
      }
      if (pulsarConsumer != null) {
        return pulsarConsumer.shutdown();
      }
    }
    return CompletableFuture.completedFuture(null);
  }

  public void queueRemoved(Queue queue) {
    String queueName = queue.getName();
    if (bindings.containsKey(queueName)) {
      Map<String, PulsarConsumer> binding = bindings.get(queueName);
      binding.values().forEach(PulsarConsumer::close);
      bindings.remove(queueName);
    }
  }

  public static AbstractExchange createExchange(
      Type type, String name, boolean durable, LifetimePolicy lifetimePolicy) {
    switch (type) {
      case direct:
        return new DirectExchange(name, durable, lifetimePolicy);
      case fanout:
        return new FanoutExchange(name, durable, lifetimePolicy);
      case topic:
        return new TopicExchange(name, durable, lifetimePolicy);
      case headers:
        return new HeadersExchange(name, durable, lifetimePolicy);
      default:
        return null;
    }
  }

  public TopicName getTopicName(String vHost, String exchangeName, String routingKey) {
    StringBuilder topic = new StringBuilder(isBlank(exchangeName) ? "amq.default" : exchangeName);
    if (isNotBlank(routingKey)) {
      topic.append("$$").append(routingKey);
    }
    return TopicName.get("persistent", NamespaceName.get(vHost), topic.toString());
  }

  @VisibleForTesting
  public Map<String, Map<String, PulsarConsumer>> getBindings() {
    return bindings;
  }

  public ExchangeMetadata toMetadata() {
    return new ExchangeMetadata(convertType(type), durable, lifetimePolicy);
  }

  public static ExchangeMetadata.Type convertType(Type type) {
    switch (type) {
      case direct:
        return ExchangeMetadata.Type.direct;
      case fanout:
        return ExchangeMetadata.Type.fanout;
      case topic:
        return ExchangeMetadata.Type.topic;
      case headers:
        return ExchangeMetadata.Type.headers;
      default:
        throw new IllegalArgumentException("Unknown exchange type");
    }
  }

  public static Type convertMetadataType(ExchangeMetadata.Type type) {
    switch (type) {
      case direct:
        return Type.direct;
      case fanout:
        return Type.fanout;
      case topic:
        return Type.topic;
      case headers:
        return Type.headers;
      default:
        throw new IllegalArgumentException("Unknown exchange type");
    }
  }

  public static AbstractExchange fromMetadata(String name, ExchangeMetadata metadata) {
    return AbstractExchange.createExchange(
        convertMetadataType(metadata.getType()),
        name,
        metadata.isDurable(),
        metadata.getLifetimePolicy());
  }
}
