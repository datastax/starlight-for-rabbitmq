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

import com.datastax.oss.pulsar.rabbitmqgw.AbstractExchange.Type;
import java.util.HashMap;
import java.util.Map;
import org.apache.qpid.server.exchange.ExchangeDefaults;
import org.apache.qpid.server.model.LifetimePolicy;

public class VirtualHost {
  private final String namespace;

  private final Map<String, AbstractExchange> exchanges = new HashMap<>();
  private final Map<String, Queue> queues = new HashMap<>();

  public VirtualHost(String namespace) {
    this.namespace = namespace;
    addStandardExchange(ExchangeDefaults.DEFAULT_EXCHANGE_NAME, Type.direct);
    addStandardExchange(ExchangeDefaults.DIRECT_EXCHANGE_NAME, Type.direct);
    addStandardExchange(ExchangeDefaults.FANOUT_EXCHANGE_NAME, Type.fanout);
    addStandardExchange(ExchangeDefaults.TOPIC_EXCHANGE_NAME, Type.topic);
    addStandardExchange(ExchangeDefaults.HEADERS_EXCHANGE_NAME, Type.headers);
  }

  public String getNamespace() {
    return namespace;
  }

  public boolean hasExchange(String name) {
    return exchanges.containsKey(name);
  }

  public AbstractExchange getExchange(String name) {
    return exchanges.get(name);
  }

  public void addExchange(AbstractExchange exchange) {
    exchanges.put(exchange.getName(), exchange);
  }

  public void deleteExchange(AbstractExchange exchange) {
    exchanges.remove(exchange.getName());
  }

  public Queue getQueue(String name) {
    return queues.get(name);
  }

  public void addQueue(Queue queue) {
    queues.put(queue.getName(), queue);
  }

  public void deleteQueue(Queue queue) {
    queues.remove(queue.getName());
  }

  private void addStandardExchange(String directExchangeName, Type type) {
    addExchange(
        AbstractExchange.createExchange(type, directExchangeName, true, LifetimePolicy.PERMANENT));
  }
}
