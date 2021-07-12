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

import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.qpid.server.model.LifetimePolicy;

public class FanoutExchange extends AbstractExchange {

  public FanoutExchange(String name, boolean durable, LifetimePolicy lifetimePolicy) {
    super(name, Type.fanout, durable, lifetimePolicy);
  }

  @Override
  public void bind(Queue queue, String routingKey, GatewayConnection connection)
      throws PulsarClientException {
    super.bind(queue, "", connection);
  }

  @Override
  public void unbind(Queue queue, String routingKey) throws PulsarClientException {
    super.unbind(queue, "");
  }

  @Override
  public boolean hasBinding(String bindingKey, Queue queue) {
    return super.hasBinding("", queue);
  }

  @Override
  public TopicName getTopicName(String vHost, String exchangeName, String routingKey) {
    return super.getTopicName(vHost, exchangeName, "");
  }
}
