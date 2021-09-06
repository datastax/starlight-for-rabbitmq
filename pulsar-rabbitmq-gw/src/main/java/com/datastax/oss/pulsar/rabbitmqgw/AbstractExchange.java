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

import com.datastax.oss.pulsar.rabbitmqgw.metadata.ExchangeMetadata;
import com.datastax.oss.pulsar.rabbitmqgw.metadata.VirtualHostMetadata;
import java.util.concurrent.CompletableFuture;
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

  public abstract CompletableFuture<Void> bind(
      VirtualHostMetadata vhost,
      String exchange,
      String queue,
      String routingKey,
      GatewayConnection connection);

  public abstract CompletableFuture<Void> unbind(
      VirtualHostMetadata vhost,
      String exchange,
      String queue,
      String routingKey,
      GatewayConnection gatewayConnection);

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
        throw new IllegalArgumentException("Unknown exchange type");
    }
  }

  public static TopicName getTopicName(String vHost, String exchangeName, String routingKey) {
    StringBuilder topic = new StringBuilder(isBlank(exchangeName) ? "amq.default" : exchangeName);
    if (isNotBlank(routingKey)) {
      topic.append("$$").append(routingKey);
    }
    return TopicName.get("persistent", NamespaceName.get(vHost), topic.toString());
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
