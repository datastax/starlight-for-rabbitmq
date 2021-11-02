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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.pulsar.client.api.Message;

public class Queue {

  private final java.util.Queue<MessageRequest> messageRequests = new ConcurrentLinkedQueue<>();
  private final java.util.Queue<PulsarConsumer.PulsarConsumerMessage> pendingBindings =
      new ConcurrentLinkedQueue<>();

  private final Map<String, PulsarConsumer> subscriptions = new ConcurrentHashMap<>();

  private volatile AMQConsumer _exclusiveSubscriber;

  private final List<AMQConsumer> consumers = new ArrayList<>();

  public Queue() {}

  public int getQueueDepthMessages() {
    // TODO: implement message count in queue ?
    return 0;
  }

  public int getConsumerCount() {
    return consumers.size();
  }

  public Map<String, PulsarConsumer> getSubscriptions() {
    return subscriptions;
  }

  public CompletableFuture<PulsarConsumer.PulsarConsumerMessage> receiveAsync(
      AMQConsumer consumer) {
    // TODO: support consumer priority
    MessageRequest request = new MessageRequest(consumer);
    messageRequests.add(request);
    deliverMessageIfAvailable();
    return request.getResponse();
  }

  public PulsarConsumer.PulsarConsumerMessage receive() {
    PulsarConsumer.PulsarConsumerMessage consumerMessage = getReadyBinding();
    if (consumerMessage != null) {
      consumerMessage.getConsumer().receiveAndDeliverMessages();
      return consumerMessage;
    }
    return null;
  }

  public PulsarConsumer.PulsarConsumerMessage getReadyBinding() {
    synchronized (this) {
      return pendingBindings.poll();
    }
  }

  public void deliverMessageIfAvailable() {
    PulsarConsumer.PulsarConsumerMessage consumerMessage = getReadyBinding();
    if (consumerMessage != null) {
      deliverMessage(consumerMessage);
    }
  }

  public void deliverMessage(PulsarConsumer.PulsarConsumerMessage consumerMessage) {
    synchronized (this) {
      if (consumerMessage != null) {
        Message<byte[]> message = consumerMessage.getMessage();
        boolean messageDelivered = false;
        while (!messageRequests.isEmpty()) {
          MessageRequest request = messageRequests.poll();
          if (request != null && !request.getResponse().isDone()) {
            boolean allocated = request.getConsumer().useCreditForMessage(message.size());
            if (allocated) {
              request.getResponse().complete(consumerMessage);
              consumerMessage.getConsumer().receiveAndDeliverMessages();
              messageDelivered = true;
              break;
            } else {
              request.getConsumer().block();
            }
          }
        }
        if (!messageDelivered) {
          pendingBindings.add(consumerMessage);
        }
      }
    }
  }

  public long clearQueue() {
    // TODO: implement queue purge
    return 0;
  }

  public static class MessageRequest {
    private final AMQConsumer consumer;
    private final CompletableFuture<PulsarConsumer.PulsarConsumerMessage> response =
        new CompletableFuture<>();

    public MessageRequest(AMQConsumer consumer) {
      this.consumer = consumer;
    }

    public CompletableFuture<PulsarConsumer.PulsarConsumerMessage> getResponse() {
      return response;
    }

    public AMQConsumer getConsumer() {
      return consumer;
    }
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
