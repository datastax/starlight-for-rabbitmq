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

import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.qpid.server.model.LifetimePolicy;

public class Exchange {

  enum Type {
    direct,
    fanout,
    topic,
    headers
  }

  private final String name;
  private final Type type;
  private final boolean durable;
  private final LifetimePolicy lifetimePolicy;
  private final Map<String, Binding> bindings = new HashMap<>();

  public Exchange(String name, Type type, boolean durable, LifetimePolicy lifetimePolicy) {
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

  public boolean hasBindings() {
    return bindings.size() != 0;
  }

  public static TopicName getTopicName(String vHost, String exchangeName, String routingKey) {
    StringBuilder topic = new StringBuilder(isBlank(exchangeName) ? "amq.default" : exchangeName);
    if (isNotBlank(routingKey)) {
      topic.append("$$").append(routingKey);
    }
    return TopicName.get("persistent", NamespaceName.get(vHost), topic.toString());
  }

  public void bind(Queue queue, String routingKey, GatewayConnection connection)
      throws PulsarClientException {
    String vHost = connection.getNamespace();

    if (!bindings.containsKey(queue.getName())) {
      bindings.put(
          queue.getName(),
          new Binding(vHost, this, queue, connection.getGatewayService().getPulsarClient()));
    }
    Binding binding = bindings.get(queue.getName());
    binding.addKey(routingKey);
    queue.addBinding(binding);
  }
}
