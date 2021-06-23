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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import org.apache.pulsar.client.api.Message;
import org.apache.qpid.server.model.ExclusivityPolicy;
import org.apache.qpid.server.model.LifetimePolicy;

public class Queue {

  private final String name;
  private final LifetimePolicy lifetimePolicy;
  private final ExclusivityPolicy exclusivityPolicy;

  private final Map<String, Binding> bindings = new ConcurrentHashMap<>();
  private final java.util.Queue<MessageRequest> messageRequests = new ConcurrentLinkedQueue<>();
  private final java.util.Queue<Binding> pendingBindings = new ConcurrentLinkedQueue<>();

  private volatile AMQConsumer _exclusiveSubscriber;
  private final List<AMQConsumer> consumers = new ArrayList<>();

  public Queue(LifetimePolicy lifetimePolicy, ExclusivityPolicy exclusivityPolicy, String name) {
    this.name = name;
    this.lifetimePolicy = lifetimePolicy;
    this.exclusivityPolicy = exclusivityPolicy;
  }

  public String getName() {
    return name;
  }

  public void addBinding(Binding binding) {
    bindings.put(binding.getExchange().getName(), binding);
  }

  public int getQueueDepthMessages() {
    // TODO: implement message count in queue ?
    return 0;
  }

  public int getConsumerCount() {
    return consumers.size();
  }

  public boolean isExclusive() {
    return exclusivityPolicy != ExclusivityPolicy.NONE;
  }

  public LifetimePolicy getLifetimePolicy() {
    return lifetimePolicy;
  }

  public CompletableFuture<MessageResponse> receiveAsync(AMQConsumer consumer) {
    // TODO: support consumer priority
    MessageRequest request = new MessageRequest(consumer);
    messageRequests.add(request);
    deliverMessageIfAvailable();
    return request.getResponse();
  }

  public MessageResponse receive() {
    Binding binding = getReadyBinding();
    if (binding != null) {
      Message<byte[]> message = binding.getReceive().join();
      binding.receiveMessageAsync().thenAcceptAsync(this::deliverMessage);
      return new MessageResponse(message, binding);
    }
    return null;
  }

  public Binding getReadyBinding() {
    return pendingBindings.poll();
  }

  public void deliverMessageIfAvailable() {
    Binding binding = getReadyBinding();
    if (binding != null) {
      deliverMessage(binding);
    }
  }

  public void deliverMessage(Binding binding) {
    Message<byte[]> message = binding.getReceive().join();
    boolean messageDelivered = false;
    while (!messageRequests.isEmpty()) {
      MessageRequest request = messageRequests.poll();
      if (request != null && !request.getResponse().isDone()) {
        boolean allocated = request.getConsumer().useCreditForMessage(message.getData().length);
        if (allocated) {
          request.getResponse().complete(new MessageResponse(message, binding));
          binding.receiveMessageAsync().thenAcceptAsync(this::deliverMessage);
          messageDelivered = true;
          break;
        } else {
          request.getConsumer().block();
        }
      }
    }
    if (!messageDelivered) {
      pendingBindings.add(binding);
    }
  }

  public static class MessageResponse {
    private final Message<byte[]> message;
    private final Binding binding;

    public MessageResponse(Message<byte[]> message, Binding binding) {
      this.message = message;
      this.binding = binding;
    }

    public Message<byte[]> getMessage() {
      return message;
    }

    public Binding getBinding() {
      return binding;
    }
  }

  public static class MessageRequest {
    private final AMQConsumer consumer;
    private final CompletableFuture<MessageResponse> response = new CompletableFuture<>();

    public MessageRequest(AMQConsumer consumer) {
      this.consumer = consumer;
    }

    public CompletableFuture<MessageResponse> getResponse() {
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

  public boolean hasExclusiveConsumer() {
    return _exclusiveSubscriber != null;
  }
}
