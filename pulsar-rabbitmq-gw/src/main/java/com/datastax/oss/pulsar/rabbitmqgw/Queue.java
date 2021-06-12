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
  private final ConcurrentLinkedQueue<CompletableFuture<Message<byte[]>>> messageRequests =
      new ConcurrentLinkedQueue<>();

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

  public CompletableFuture<Message<byte[]>> receiveAsync() {
    CompletableFuture<Message<byte[]>> messageRequest = new CompletableFuture<>();
    messageRequests.add(messageRequest);
    deliverMessageIfAvailable();
    return messageRequest;
  }

  public void deliverMessageIfAvailable() {
    if (exchangesToPoll.size() == 0) {
      return;
    }
    if (currentExchange == null || !currentExchange.hasNext()) {
      currentExchange = exchangesToPoll.iterator();
    }
    String s = currentExchange.next();
    Binding binding = bindings.get(s);
    CompletableFuture<Message<byte[]>> receive = binding.getReceive();
    if (receive.isDone()) {
      CompletableFuture<Message<byte[]>> request = messageRequests.poll();
      if (request != null) {
        Message<byte[]> message = null;
        try {
          message = receive.get();
        } catch (Exception e) {
          // TODO: close connection ?
        }
        binding.receiveMessageAsync().thenRunAsync(this::deliverMessageIfAvailable);
        request.complete(message);
      }
    }
  }
}
