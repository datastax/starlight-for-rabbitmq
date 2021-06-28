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

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.Message;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;

public class AMQConsumer {

  enum State {
    OPEN,
    CLOSED
  }

  private final AMQChannel channel;
  private final AMQShortString tag;
  private final Queue queue;
  private final boolean noAck;
  private final AtomicReference<State> _state = new AtomicReference<>(State.OPEN);
  private CompletableFuture<Queue.MessageResponse> messageCompletableFuture;
  private final AtomicBoolean blocked = new AtomicBoolean(false);

  public AMQConsumer(AMQChannel channel, AMQShortString tag, Queue queue, boolean noAck) {
    this.channel = channel;
    this.tag = tag;
    this.queue = queue;
    this.noAck = noAck;
  }

  public void consume() {
    if (!blocked.get()) {
      messageCompletableFuture = queue.receiveAsync(this);
      messageCompletableFuture
          .thenAccept(
              messageResponse -> {
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
                  messageResponse.getBinding().ackMessage(message);
                } else {
                  channel.addUnacknowledgedMessage(
                      message.getMessageId(),
                      this,
                      deliveryTag,
                      messageResponse.getBinding(),
                      contentBody.getSize());
                }
              })
          .thenRunAsync(this::consume);
    }
  }

  public boolean useCreditForMessage(int length) {
    return noAck || channel.getCreditManager().useCreditForMessage(length);
  }

  public boolean close() {
    if (_state.compareAndSet(State.OPEN, State.CLOSED)) {
      block();
      queue.unregisterConsumer(this);
      return true;
    } else {
      return false;
    }
  }

  public void block() {
    blocked.set(true);
    messageCompletableFuture.cancel(true);
  }

  public void unblock() {
    if (_state.get() == State.OPEN && blocked.get()) {
      blocked.set(false);
      consume();
    }
  }
}
