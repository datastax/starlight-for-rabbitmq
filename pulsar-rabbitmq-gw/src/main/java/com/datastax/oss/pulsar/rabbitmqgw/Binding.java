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
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

public class Binding {

  private final String vHost;
  private final Exchange exchange;
  private final Queue queue;
  private final List<String> routingKeys = new ArrayList<>();
  private final PulsarClient pulsarClient;

  private Consumer<byte[]> pulsarConsumer;
  private volatile MessageId lastReceivedmessageId;
  private volatile CompletableFuture<Message<byte[]>> message;

  public Binding(String vHost, Exchange exchange, Queue queue, PulsarClient pulsarClient) {
    this.vHost = vHost;
    this.exchange = exchange;
    this.queue = queue;
    this.pulsarClient = pulsarClient;
  }

  public Exchange getExchange() {
    return exchange;
  }

  public CompletableFuture<Binding> receiveMessageAsync() {
    CompletableFuture<Message<byte[]>> messageCompletableFuture = pulsarConsumer.receiveAsync();
    message =
        messageCompletableFuture.thenApply(
            msg -> {
              lastReceivedmessageId = msg.getMessageId();
              return msg;
            });
    return message.thenApply(it -> this);
  }

  public void addKey(String routingKey) throws PulsarClientException {
    routingKeys.add(routingKey);
    List<String> topics =
        routingKeys
            .stream()
            .map(key -> Exchange.getTopicName(vHost, exchange.getName(), routingKey).toString())
            .collect(Collectors.toList());
    if (pulsarConsumer != null) {
      // TODO: verify that it also removes the subscription
      pulsarConsumer.close();
    }
    // TODO: make this part async
    pulsarConsumer =
        pulsarClient
            .newConsumer()
            .topics(topics)
            // TODO: subscription name
            .subscriptionName(exchange.getName() + "-" + queue.getName() + "-" + UUID.randomUUID())
            .subscriptionType(SubscriptionType.Shared)
            .negativeAckRedeliveryDelay(0, TimeUnit.MILLISECONDS)
            .subscribe();
    if (lastReceivedmessageId != null) {
      pulsarConsumer.seek(lastReceivedmessageId);
    }
    receiveMessageAsync().thenAccept(queue::deliverMessage);
  }

  public CompletableFuture<Message<byte[]>> getReceive() {
    return message;
  }

  public CompletableFuture<Void> ackMessage(Message<byte[]> message) {
    return pulsarConsumer.acknowledgeAsync(message);
  }

  public CompletableFuture<Void> ackMessage(MessageId messageId) {
    return pulsarConsumer.acknowledgeAsync(messageId);
  }

  public void nackMessage(MessageId messageId) {
    pulsarConsumer.negativeAcknowledge(messageId);
  }

  @Override
  public String toString() {
    return "Binding{" + "exchange=" + exchange.getName() + ", queue=" + queue.getName() + '}';
  }
}
