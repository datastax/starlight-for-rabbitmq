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

import static org.junit.Assert.*;

import com.datastax.oss.pulsar.rabbitmqtests.javaclient.BrokerTestCase;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.impl.recovery.AutorecoveringConnection;
import org.junit.Test;

public class Heartbeat extends BrokerTestCase {

  @Override
  protected ConnectionFactory newConnectionFactory() {
    ConnectionFactory cf = super.newConnectionFactory();
    cf.setRequestedHeartbeat(1);
    return cf;
  }

  @Test
  public void heartbeat() throws InterruptedException {
    assertEquals(1, connection.getHeartbeat());
    Thread.sleep(3100);
    assertTrue(connection.isOpen());
    ((AutorecoveringConnection) connection).getDelegate().setHeartbeat(0);
    assertEquals(0, connection.getHeartbeat());
    Thread.sleep(3100);
    assertFalse(connection.isOpen());
  }
}
