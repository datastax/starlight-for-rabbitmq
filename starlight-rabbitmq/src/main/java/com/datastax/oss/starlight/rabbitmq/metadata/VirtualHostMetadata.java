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
package com.datastax.oss.starlight.rabbitmq.metadata;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class VirtualHostMetadata {

  private String namespace;
  private final Map<String, ExchangeMetadata> exchanges = new ConcurrentHashMap<>();
  private final Map<String, QueueMetadata> queues = new ConcurrentHashMap<>();

  private final Map<String, Map<String, BindingMetadata>> subscriptions = new ConcurrentHashMap<>();

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
  }

  public Map<String, ExchangeMetadata> getExchanges() {
    return exchanges;
  }

  public Map<String, QueueMetadata> getQueues() {
    return queues;
  }

  public Map<String, Map<String, BindingMetadata>> getSubscriptions() {
    return subscriptions;
  }
}
