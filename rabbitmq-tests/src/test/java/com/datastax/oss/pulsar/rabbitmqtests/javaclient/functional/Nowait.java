// Copyright (c) 2007-2020 VMware, Inc. or its affiliates.  All rights reserved.
//
// This software, the RabbitMQ Java client library, is triple-licensed under the
// Mozilla Public License 2.0 ("MPL"), the GNU General Public License version 2
// ("GPL") and the Apache License version 2 ("ASL"). For the MPL, please see
// LICENSE-MPL-RabbitMQ. For the GPL, please see LICENSE-GPL2.  For the ASL,
// please see LICENSE-APACHE2.
//
// This software is distributed on an "AS IS" basis, WITHOUT WARRANTY OF ANY KIND,
// either express or implied. See the LICENSE file for specific language governing
// rights and limitations of this software.
//
// If you have any questions regarding licensing, please contact us at
// info@rabbitmq.com.

package com.datastax.oss.pulsar.rabbitmqtests.javaclient.functional;

import static org.junit.jupiter.api.Assertions.assertTrue;
import com.datastax.oss.pulsar.rabbitmqtests.javaclient.BrokerTestCase;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.ExecutionException;
import org.awaitility.Awaitility;
import org.junit.Ignore;
import org.junit.Test;

public class Nowait extends BrokerTestCase {

  @Test
  public void testQueueDeclareWithNowait() throws Exception {
    String q = generateQueueName();
    channel.queueDeclareNoWait(q, false, true, true, null);

    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> gatewayService.getContextMetadata().model().getVhosts().get("public/default").getQueues().containsKey(q));

    channel.queueDeclarePassive(q);
  }

  @Test
  public void testQueueBindWithNowait() throws Exception {
    String q = generateQueueName();
    channel.queueDeclare(q, false, true, true, null);
    channel.queueBindNoWait(q, "amq.fanout", "", null);

    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> gatewayService.getContextMetadata().model().getVhosts().get("public/default").getExchanges().get("amq.fanout")
        .getBindings().containsKey(q));
  }

  @Test
  public void testExchangeDeclareWithNowait() throws Exception {
    String x = generateExchangeName();
    try {
      channel.exchangeDeclareNoWait(x, "fanout", false, false, false, null);

      Awaitility.await()
          .atMost(Duration.ofSeconds(5))
          .until(() -> gatewayService.getContextMetadata().model().getVhosts().get("public/default").getExchanges().containsKey(x));

      channel.exchangeDeclarePassive(x);
    } finally {
      channel.exchangeDelete(x);
    }
  }

  @Test
  @Ignore("Need to implement exchange to exchange bindings first")
  public void testExchangeBindWithNowait() throws IOException {
    String x = generateExchangeName();
    try {
      channel.exchangeDeclareNoWait(x, "fanout", false, false, false, null);
      channel.exchangeBindNoWait(x, "amq.fanout", "", null);
    } finally {
      channel.exchangeDelete(x);
    }
  }

  @Test
  @Ignore("Need to implement exchange to exchange bindings first")
  public void testExchangeUnbindWithNowait() throws IOException {
    String x = generateExchangeName();
    try {
      channel.exchangeDeclare(x, "fanout", false, false, false, null);
      channel.exchangeBind(x, "amq.fanout", "", null);
      channel.exchangeUnbindNoWait(x, "amq.fanout", "", null);
    } finally {
      channel.exchangeDelete(x);
    }
  }

  @Test
  public void testQueueDeleteWithNowait() throws Exception {
    String q = generateQueueName();
    channel.queueDeclare(q, false, true, true, null);
    assertTrue(gatewayService.getContextMetadata().model().getVhosts().get("public/default").getQueues().containsKey(q));

    channel.queueDeleteNoWait(q, false, false);

    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> !gatewayService.getContextMetadata().model().getVhosts().get("public/default").getQueues().containsKey(q));
  }

  @Test
  public void testExchangeDeleteWithNowait() throws Exception {
    String x = generateExchangeName();
    channel.exchangeDeclare(x, "fanout", false, false, false, null);
    assertTrue(gatewayService.getContextMetadata().model().getVhosts().get("public/default").getExchanges().containsKey(x));

    channel.exchangeDeleteNoWait(x, false);
    Awaitility.await()
        .atMost(Duration.ofSeconds(5))
        .until(() -> !gatewayService.getContextMetadata().model().getVhosts().get("public/default").getExchanges().containsKey(x));
  }
}
