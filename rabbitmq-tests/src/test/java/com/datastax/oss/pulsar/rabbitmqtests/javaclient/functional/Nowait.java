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

import com.datastax.oss.pulsar.rabbitmqtests.javaclient.BrokerTestCase;
import java.io.IOException;
import org.junit.Ignore;
import org.junit.Test;

public class Nowait extends BrokerTestCase {

  @Test
  public void testQueueDeclareWithNowait() throws Exception {
    String q = generateQueueName();
    channel.queueDeclareNoWait(q, false, true, true, null);

    // Pulsar-RMQ edit: give some time for the queue to be created
    long now = System.currentTimeMillis();
    while (System.currentTimeMillis() - now < 5000) {
      try {
        channel.queueDeclarePassive(q);
        break;
      } catch (IOException e) {
        Thread.sleep(10);
        channel = connection.createChannel();
      }
    }

    channel.queueDeclarePassive(q);
  }

  @Test
  public void testQueueBindWithNowait() throws Exception {
    String q = generateQueueName();
    channel.queueDeclare(q, false, true, true, null);
    channel.queueBindNoWait(q, "amq.fanout", "", null);
  }

  @Test
  public void testExchangeDeclareWithNowait() throws Exception {
    String x = generateExchangeName();
    try {
      channel.exchangeDeclareNoWait(x, "fanout", false, false, false, null);

      // Pulsar-RMQ edit: give some time for the exchange to be created
      long now = System.currentTimeMillis();
      while (System.currentTimeMillis() - now < 5000) {
        try {
          channel.exchangeDeclarePassive(x);
          break;
        } catch (IOException e) {
          Thread.sleep(10);
          channel = connection.createChannel();
        }
      }

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
    channel.queueDeleteNoWait(q, false, false);
  }

  @Test
  public void testExchangeDeleteWithNowait() throws Exception {
    String x = generateExchangeName();
    channel.exchangeDeclare(x, "fanout", false, false, false, null);
    channel.exchangeDeleteNoWait(x, false);
  }
}
