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

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.MessageId;

public abstract class MessageConsumerAssociation {
  private final MessageId messageId;
  private final int size;

  MessageConsumerAssociation(MessageId messageId, int size) {
    this.messageId = messageId;
    this.size = size;
  }

  public MessageId getMessageId() {
    return messageId;
  }

  public int getSize() {
    return size;
  }

  public abstract boolean isUsesCredit();

  public abstract CompletableFuture<Void> ack();

  public abstract void requeue();
}
