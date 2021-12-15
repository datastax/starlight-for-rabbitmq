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

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class Queue {

  private volatile AMQConsumer _exclusiveSubscriber;

  private final List<AMQConsumer> consumers = new CopyOnWriteArrayList<>();

  public Queue() {}

  public int getQueueDepthMessages() {
    // TODO: implement message count in queue ?
    return 0;
  }

  public int getConsumerCount() {
    return consumers.size();
  }

  public long clearQueue() {
    // TODO: implement queue purge
    return 0;
  }

  public void addConsumer(AMQConsumer consumer, boolean exclusive) {
    if (exclusive) {
      _exclusiveSubscriber = consumer;
    }
    consumers.add(consumer);
    consumer.consume();
  }

  public void unregisterConsumer(AMQConsumer consumer) {
    consumers.remove(consumer);
    _exclusiveSubscriber = null;
  }

  public List<AMQConsumer> getConsumers() {
    return consumers;
  }

  public boolean hasExclusiveConsumer() {
    return _exclusiveSubscriber != null;
  }
}
