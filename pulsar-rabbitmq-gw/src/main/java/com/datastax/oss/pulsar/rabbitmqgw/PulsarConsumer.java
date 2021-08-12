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

import io.netty.channel.EventLoop;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;

public class PulsarConsumer {

  private final String exchange;
  private final String topic;
  private final GatewayService gatewayService;
  private final PulsarAdmin pulsarAdmin;
  private final Queue queue;
  private volatile Consumer<byte[]> pulsarConsumer;
  private final String subscriptionName;

  private volatile MessageId lastMessageId;

  private volatile ScheduledFuture<?> scheduledFuture;

  private final EventLoop eventLoop;
  private final AtomicBoolean closing;

  PulsarConsumer(
      String exchange, String topic, String subscriptionName, GatewayService service, Queue queue) {
    this.exchange = exchange;
    this.topic = topic;
    this.subscriptionName = subscriptionName;
    this.gatewayService = service;
    this.pulsarAdmin = gatewayService.getPulsarAdmin();
    this.queue = queue;
    this.eventLoop = service.getWorkerGroup().next();
    this.closing = new AtomicBoolean(false);
  }

  public CompletableFuture<Void> subscribe() {
    return gatewayService
        .getPulsarClient()
        .newConsumer()
        .topic(topic)
        .subscriptionName(subscriptionName)
        .subscriptionType(SubscriptionType.Shared)
        .negativeAckRedeliveryDelay(0, TimeUnit.MILLISECONDS)
        .subscribeAsync()
        .thenAccept(
            consumer -> {
              if (!closing.get()) {
                this.pulsarConsumer = consumer;
              } else {
                consumer.closeAsync();
              }
            });
  }

  private CompletableFuture<PulsarConsumerMessage> receiveMessageAsync() {
    return pulsarConsumer
        .receiveAsync()
        .thenApply(
            msg -> {
              if (lastMessageId != null) {
                if (msg.getMessageId().compareTo(lastMessageId) > 0) {
                  // No more consumption needed. Now wait for acks before closing.
                  pulsarConsumer.acknowledgeAsync(msg);

                  // Drain the receiver queue
                  pulsarConsumer.pause();
                  Message<byte[]> message = null;
                  do {
                    try {
                      message = pulsarConsumer.receive(0, TimeUnit.SECONDS);
                    } catch (PulsarClientException e) {
                      // TODO: handle exception
                    }
                    if (message != null) {
                      if (message.getMessageId().compareTo(lastMessageId) > 0) {
                        pulsarConsumer.acknowledgeAsync(message);
                      } else {
                        return new PulsarConsumerMessage(message, this);
                      }
                    }
                  } while (message != null);

                  // Receive messages again after some time to check if we get unacked messages
                  // Note: unacked messages are sent in priority by the broker
                  this.eventLoop.schedule(this::resumeConsumption, 100, TimeUnit.MILLISECONDS);
                  return null;
                } else {
                  pulsarConsumer.resume();
                }
              }
              return new PulsarConsumerMessage(msg, this);
            });
  }

  public String getExchange() {
    return exchange;
  }

  public void setLastMessageId(MessageId lastMessageId) {
    this.lastMessageId = lastMessageId;
  }

  public CompletableFuture<Void> receiveAndDeliverMessages() {
    return receiveMessageAsync().thenAcceptAsync(queue::deliverMessage, this.eventLoop);
  }

  private CompletableFuture<Void> resumeConsumption() {
    pulsarConsumer.resume();
    return receiveAndDeliverMessages();
  }

  public CompletableFuture<Void> shutdown() {
    // TODO: prevent multiple shutdowns
    CompletableFuture<MessageId> lastMessageIdFuture;
    if (pulsarConsumer != null) {
      lastMessageIdFuture = pulsarConsumer.getLastMessageIdAsync();
    } else {
      lastMessageIdFuture = pulsarAdmin.topics().getLastMessageIdAsync(topic);
    }
    return lastMessageIdFuture.thenAccept(
        messageId -> {
          lastMessageId = messageId;
          scheduledFuture =
              this.eventLoop.scheduleAtFixedRate(
                  this::checkIfSubscriptionCanBeRemoved, 1, 1, TimeUnit.SECONDS);
        });
  }

  public void close() {
    closing.set(true);
    if (pulsarConsumer != null) {
      pulsarConsumer.closeAsync();
    }
    pulsarAdmin.topics().deleteSubscriptionAsync(topic, subscriptionName, true);
    if (scheduledFuture != null) {
      scheduledFuture.cancel(false);
    }
  }

  private void checkIfSubscriptionCanBeRemoved() {
    pulsarAdmin
        .topics()
        .peekMessagesAsync(topic, subscriptionName, 1)
        .whenComplete(
            (messages, throwable) -> {
              if (throwable != null) {
                // TODO: log error and close channel
                scheduledFuture.cancel(false);
                return;
              }
              if (messages.size() > 0) {
                Message<byte[]> message = messages.get(0);
                if (message.getMessageId().compareTo(lastMessageId) <= 0) {
                  return;
                }
              }
              // All messages have been acked, can close the consumer
              close();
            });
  }

  public CompletableFuture<Void> ackMessage(MessageId messageId) {
    return pulsarConsumer.acknowledgeAsync(messageId);
  }

  public void nackMessage(MessageId messageId) {
    pulsarConsumer.negativeAcknowledge(messageId);
  }

  public static class PulsarConsumerMessage {
    private final Message<byte[]> message;
    private final PulsarConsumer consumer;

    public PulsarConsumerMessage(Message<byte[]> message, PulsarConsumer consumer) {
      this.message = message;
      this.consumer = consumer;
    }

    public Message<byte[]> getMessage() {
      return message;
    }

    public PulsarConsumer getConsumer() {
      return consumer;
    }
  }
}
