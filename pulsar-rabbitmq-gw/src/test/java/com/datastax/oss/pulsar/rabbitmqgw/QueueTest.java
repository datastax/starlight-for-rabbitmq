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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.qpid.server.model.ExclusivityPolicy;
import org.apache.qpid.server.model.LifetimePolicy;
import org.junit.jupiter.api.Test;

public class QueueTest extends AbstractBaseTest {

  @Test
  void testQueueReceive() throws Exception {
    openConnection();
    Queue queue = new Queue("test-queue", LifetimePolicy.PERMANENT, ExclusivityPolicy.NONE);
    Exchange exchange =
        new Exchange("test-exchange", Exchange.Type.direct, true, LifetimePolicy.PERMANENT);

    MessageImpl message1 = mock(MessageImpl.class);
    MessageImpl message2 = mock(MessageImpl.class);
    when(consumer.receiveAsync())
        .thenReturn(
            CompletableFuture.completedFuture(message1),
            CompletableFuture.completedFuture(message2));
    exchange.bind(queue, queue.getName(), connection);

    Message<byte[]> msg = queue.receiveAsync(true).get();
    assertEquals(message1, msg);
    msg = queue.receiveAsync(true).get();
    assertEquals(message2, msg);
  }
}
