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
import java.util.concurrent.atomic.AtomicReference;
import org.apache.pulsar.client.api.Message;
import org.apache.qpid.server.protocol.v0_8.AMQShortString;
import org.apache.qpid.server.protocol.v0_8.transport.ContentBody;

public class ConsumerTarget {

  enum State {
    OPEN,
    CLOSED
  }

  private final AMQChannel channel;
  private final AMQShortString tag;
  private final Queue queue;
  private final AtomicReference<State> _state = new AtomicReference<>(State.OPEN);
  private CompletableFuture<Message<byte[]>> messageCompletableFuture;

  public ConsumerTarget(AMQChannel channel, AMQShortString tag, Queue queue) {
    this.channel = channel;
    this.tag = tag;
    this.queue = queue;
  }

  public void consume() {
    if (_state.get() == State.OPEN) {
      messageCompletableFuture = queue.receiveAsync(true, 0);
      messageCompletableFuture
          .thenAccept(
              message ->
                  channel
                      .getConnection()
                      .getProtocolOutputConverter()
                      .writeDeliver(
                          MessageUtils.getMessagePublishInfo(message),
                          new ContentBody(ByteBuffer.wrap(message.getData())),
                          MessageUtils.getContentHeaderBody(message),
                          message.getRedeliveryCount() > 0,
                          channel.getChannelId(),
                          channel.getNextDeliveryTag(),
                          tag))
          .thenRunAsync(this::consume);
    }
  }

  public boolean close() {
    if (_state.compareAndSet(State.OPEN, State.CLOSED)) {
      messageCompletableFuture.cancel(false);
      queue.unregisterConsumer(this);
      return true;
    } else {
      return false;
    }
  }
}
