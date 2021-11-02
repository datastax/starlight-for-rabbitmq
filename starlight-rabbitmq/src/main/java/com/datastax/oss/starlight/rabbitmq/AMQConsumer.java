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

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.Message;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;

public class AMQConsumer {

  enum State {
    OPEN,
    CLOSED;
  }

  private final AMQChannel channel;

  private final AMQShortString tag;
  private final Queue queue;
  private final boolean noAck;
  private final AtomicReference<State> _state = new AtomicReference<>(State.OPEN);
  private CompletableFuture<PulsarConsumer.PulsarConsumerMessage> messageCompletableFuture;
  private final AtomicBoolean blocked = new AtomicBoolean(false);
  private final java.util.Queue<PulsarConsumer.PulsarConsumerMessage> pendingBindings =
      new ConcurrentLinkedQueue<>();

  private final Map<String, PulsarConsumer> subscriptions = new ConcurrentHashMap<>();

  public AMQConsumer(AMQChannel channel, AMQShortString tag, Queue queue, boolean noAck) {
    this.channel = channel;
    this.tag = tag;
    this.queue = queue;
    this.noAck = noAck;
  }

  public synchronized void deliverMessage(PulsarConsumer.PulsarConsumerMessage consumerMessage) {
    if (consumerMessage != null) {
      Message<byte[]> message = consumerMessage.getMessage();
      boolean allocated = false;
      if (messageCompletableFuture != null && !messageCompletableFuture.isDone()) {
        allocated = useCreditForMessage(message.size());
        if (allocated) {
          messageCompletableFuture.complete(consumerMessage);
          consumerMessage.getConsumer().receiveAndDeliverMessages();
        } else {
          block();
        }
      }
      if (!allocated) {
        pendingBindings.add(consumerMessage);
      }
    }
  }

  public synchronized void consume() {
    if (!blocked.get()) {
      messageCompletableFuture = new CompletableFuture<>();
      messageCompletableFuture
          .thenAccept(this::handleMessage)
          .thenRunAsync(this::consume, channel.getConnection().getGatewayService().getExecutor());

      deliverMessage(pendingBindings.poll());
    }
  }

  private synchronized void handleMessage(PulsarConsumer.PulsarConsumerMessage messageResponse) {
    if (!blocked.get()) {
      Message<byte[]> message = messageResponse.getMessage();
      long deliveryTag = channel.getNextDeliveryTag();
      ContentBody contentBody = new ContentBody(ByteBuffer.wrap(message.getData()));
      channel
          .getConnection()
          .getProtocolOutputConverter()
          .writeDeliver(
              MessageUtils.getMessagePublishInfo(message),
              contentBody,
              MessageUtils.getContentHeaderBody(message),
              message.getRedeliveryCount() > 0,
              channel.getChannelId(),
              deliveryTag,
              tag);

      if (noAck) {
        messageResponse.getConsumer().ackMessage(message.getMessageId());
      } else {
        MessageConsumerAssociation messageConsumerAssociation =
            new BasicConsumeMessageConsumerAssociation(
                message.getMessageId(), messageResponse.getConsumer(), contentBody.getSize());
        channel.addUnacknowledgedMessage(deliveryTag, messageConsumerAssociation);
      }
    } else {
      deliverMessage(messageResponse);
      channel.getCreditManager().restoreCredit(1, messageResponse.getMessage().size());
      unblock();
      throw new CancellationException();
    }
  }

  public boolean useCreditForMessage(int length) {
    boolean allocated = channel.getCreditManager().useCreditForMessage(length);
    if (allocated && noAck) {
      channel.getCreditManager().restoreCredit(1, length);
    }
    return allocated;
  }

  public boolean close() {
    if (_state.compareAndSet(State.OPEN, State.CLOSED)) {
      block();
      queue.unregisterConsumer(this);
      subscriptions.values().forEach(PulsarConsumer::close);
      return true;
    } else {
      return false;
    }
  }

  public void block() {
    synchronized (this) {
      blocked.set(true);
      messageCompletableFuture.cancel(false);
    }
  }

  public void unblock() {
    if (_state.get() == State.OPEN && blocked.get()) {
      blocked.set(false);
      consume();
    }
  }

  public PulsarConsumer startSubscription(
      String subscriptionName, String topic, GatewayService gatewayService) {
    return subscriptions.computeIfAbsent(
        subscriptionName,
        subscription -> {
          PulsarConsumer pc = new PulsarConsumer(topic, subscription, gatewayService, this);
          pc.subscribe()
              .thenRun(pc::receiveAndDeliverMessages)
              .exceptionally(
                  t -> {
                    PulsarConsumer removed = subscriptions.remove(subscription);
                    removed.close();
                    return null;
                  });
          return pc;
        });
  }
}
