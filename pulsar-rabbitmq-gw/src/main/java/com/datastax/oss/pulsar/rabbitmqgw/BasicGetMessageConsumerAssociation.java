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

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.MessageId;

public class BasicGetMessageConsumerAssociation extends MessageConsumerAssociation {
  private final MessageId messageId;
  private final Consumer<byte[]> consumer;
  private final int size;

  BasicGetMessageConsumerAssociation(MessageId messageId, Consumer<byte[]> consumer, int size) {
    super(messageId, size);
    this.messageId = messageId;
    this.consumer = consumer;
    this.size = size;
  }

  public MessageId getMessageId() {
    return messageId;
  }

  public int getSize() {
    return size;
  }

  public boolean isUsesCredit() {
    return false;
  }

  public CompletableFuture<Void> ack() {
    return consumer.acknowledgeAsync(messageId).thenCompose(it -> consumer.closeAsync());
  }

  public void requeue() {
    consumer.closeAsync();
  }
}
