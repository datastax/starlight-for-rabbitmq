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

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import io.prometheus.client.CollectorRegistry;
import org.junit.jupiter.api.Test;

public class MetricsTest extends AbstractBaseTest {

  @Test
  void testBytesInOutMetrics() {
    CollectorRegistry registry = CollectorRegistry.defaultRegistry;
    assertNull(registry.getSampleValue("server_rabbitmq_in_bytes", new String[]{"namespace"},
        new String[]{"public/default"}));
    assertNull(registry.getSampleValue("server_rabbitmq_out_bytes", new String[]{"namespace"},
        new String[]{"public/default"}));

    openConnection();
    sendChannelOpen();

    assertTrue(registry.getSampleValue("server_rabbitmq_in_bytes", new String[]{"namespace"},
        new String[]{"public/default"}) > 0);
    assertTrue(registry.getSampleValue("server_rabbitmq_out_bytes", new String[]{"namespace"},
        new String[]{"public/default"}) > 0);
  }
}
