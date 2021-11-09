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
package com.datastax.oss.starlight.rabbitmqtests;

/**
 * Test category for system tests.
 * <p>
 * System tests are able to run both with local installed Pulsar cluster (e.g. testcontainers)
 * and with an external Pulsar cluster.
 * <p>
 * Note: it is expected the external Pulsar cluster to have default configuration with the <code>rabbitmq</code> protocol handler active.
 */
public interface SystemTest {
    /**
     * If true, the test must run using an external Pulsar cluster.
     */
    boolean enabled = Boolean.parseBoolean(System.getProperty("tests.systemtests.enabled", "false"));
    /**
     * The host to connect used by the client (Broker/Proxy host).
     */
    String host = System.getProperty("tests.systemtests.pulsar.host", "localhost");
    /**
     * The AMQP listener port exposed on Pulsar Broker/Proxy.
     */
    int listenerPort = Integer.parseInt(System.getProperty("tests.systemtests.pulsar.ampqlistener.port", "5672"));

}
