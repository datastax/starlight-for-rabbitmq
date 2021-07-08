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

import com.google.common.annotations.VisibleForTesting;
import java.util.HashMap;
import java.util.Map;
import org.apache.pulsar.client.admin.PulsarAdminException;
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

  private final Map<String, Map<String, PulsarConsumer>> bindings = new HashMap<>();

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
      bindings.put(queue.getName(), new HashMap<>());
      queue.getBoundExchanges().add(this);
    }
    Map<String, PulsarConsumer> binding = bindings.get(queue.getName());
    if (!binding.containsKey(routingKey)) {
      PulsarConsumer pulsarConsumer =
          new PulsarConsumer(
              getTopicName(vHost, name, routingKey).toString(),
              connection.getGatewayService(),
              queue);
      pulsarConsumer.receiveAndDeliverMessages();
      binding.put(routingKey, pulsarConsumer);
    }
  }

  public void unbind(Queue queue, String routingKey)
      throws PulsarClientException, PulsarAdminException {
    String queueName = queue.getName();
    if (bindings.containsKey(queueName)) {
      Map<String, PulsarConsumer> binding = bindings.get(queueName);
      if (binding.containsKey(routingKey)) {
        PulsarConsumer pulsarConsumer = binding.get(routingKey);
        pulsarConsumer.shutdown();
        binding.remove(routingKey);
      }
      if (binding.size() == 0) {
        bindings.remove(queueName);
        queue.getBoundExchanges().remove(this);
      }
    }
  }

  public void queueRemoved(Queue queue) {
    String queueName = queue.getName();
    if (bindings.containsKey(queueName)) {
      Map<String, PulsarConsumer> binding = bindings.get(queueName);
      binding.values().forEach(PulsarConsumer::close);
      bindings.remove(queueName);
    }
  }

  public boolean hasBinding(String bindingKey, final Queue queue) {
    if (bindingKey == null) {
      bindingKey = "";
    }
    return bindings.containsKey(queue.getName())
        && bindings.get(queue.getName()).containsKey(bindingKey);
  }

  @VisibleForTesting
  public Map<String, Map<String, PulsarConsumer>> getBindings() {
    return bindings;
  }
}
