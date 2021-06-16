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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
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
  private final Set<String> exchangesToPoll = new HashSet<>();
  private Iterator<String> currentExchange;
  private final ConcurrentLinkedQueue<MessageRequest> messageRequests =
      new ConcurrentLinkedQueue<>();
  private final ConcurrentLinkedQueue<Binding> pendingBindings = new ConcurrentLinkedQueue<>();

  public Queue(String name, LifetimePolicy lifetimePolicy, ExclusivityPolicy exclusivityPolicy) {
    this.name = name;
    this.lifetimePolicy = lifetimePolicy;
    this.exclusivityPolicy = exclusivityPolicy;
  }

  public String getName() {
    return name;
  }

  public void addBinding(Binding binding) {
    bindings.put(binding.getExchange().getName(), binding);
    exchangesToPoll.add(binding.getExchange().getName());
  }

  public int getQueueDepthMessages() {
    // TODO: implement message count in queue ?
    return 0;
  }

  public int getConsumerCount() {
    // TODO: implement consumer count
    return 0;
  }

  public boolean isExclusive() {
    return exclusivityPolicy != ExclusivityPolicy.NONE;
  }

  public LifetimePolicy getLifetimePolicy() {
    return lifetimePolicy;
  }

  public CompletableFuture<Message<byte[]>> receiveAsync(boolean autoAck) {
    MessageRequest messageRequest = new MessageRequest(autoAck);
    messageRequests.add(messageRequest);
    deliverMessageIfAvailable();
    return messageRequest.getMessage();
  }

  public Message<byte[]> receive(boolean autoAck) {
    Binding binding = getReadyBinding();
    if (binding != null) {
      Message<byte[]> message = null;
      try {
        message = binding.getReceive().get();
      } catch (Exception e) {
        // TODO: should not happen. Close connection ?
      }
      if (autoAck) {
        binding.ackMessage(message);
      }
      binding.receiveMessageAsync().thenAcceptAsync(this::deliverMessage);
      return message;
    }
    return null;
  }

  public Binding getReadyBinding() {
    return pendingBindings.poll();
  }

  public void deliverMessageIfAvailable() {
    Binding binding = getReadyBinding();
    if (binding != null) {
      MessageRequest request = messageRequests.poll();
      if (request != null) {
        binding
            .getReceive()
            .thenAccept(
                message -> {
                  request.getMessage().complete(message);
                  if (request.isAutoAck()) {
                    binding.ackMessage(message);
                  }
                  binding.receiveMessageAsync().thenAcceptAsync(this::deliverMessage);
                });
      }
    }
  }

  public void deliverMessage(Binding binding) {
    MessageRequest request = messageRequests.poll();
    if (request != null) {
      binding
          .getReceive()
          .thenAccept(
              message -> {
                request.getMessage().complete(message);
                if (request.isAutoAck()) {
                  binding.ackMessage(message);
                }
                binding.receiveMessageAsync().thenAcceptAsync(this::deliverMessage);
              });
    } else {
      pendingBindings.add(binding);
    }
  }

  public static class MessageRequest {
    private final CompletableFuture<Message<byte[]>> message = new CompletableFuture<>();
    private final boolean autoAck;

    public MessageRequest(boolean autoAck) {
      this.autoAck = autoAck;
    }

    public CompletableFuture<Message<byte[]>> getMessage() {
      return message;
    }

    public boolean isAutoAck() {
      return autoAck;
    }
  }
}
