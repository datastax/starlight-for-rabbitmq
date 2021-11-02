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

import static com.datastax.oss.starlight.rabbitmq.ConfigurationUtils.convertFrom;
import static org.apache.commons.lang3.StringUtils.isBlank;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.authentication.AuthenticationService;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;
import org.apache.pulsar.proxy.extensions.ProxyExtension;
import org.apache.pulsar.proxy.server.ProxyConfiguration;
import org.apache.pulsar.proxy.server.ProxyService;

public class GatewayProxyProtocolHandler implements ProxyExtension {

  private static final String PROTOCOL_NAME = "rabbitmq";

  private GatewayConfiguration config;
  private GatewayService service;

  @Override
  public String extensionName() {
    return PROTOCOL_NAME;
  }

  @Override
  public boolean accept(String protocol) {
    return PROTOCOL_NAME.equals(protocol);
  }

  @SneakyThrows
  @Override
  public void initialize(ProxyConfiguration conf) {
    config = ConfigurationUtils.create(conf.getProperties(), GatewayConfiguration.class);
  }

  @SneakyThrows
  @Override
  public void start(ProxyService proxyService) {
    if (isBlank(config.getBrokerServiceURL())) {
      List<? extends ServiceLookupData> availableBrokers =
          proxyService.getDiscoveryProvider().getAvailableBrokers();
      if (availableBrokers.size() == 0) {
        throw new PulsarServerException("No active broker is available");
      }
      ServiceLookupData lookupData = availableBrokers.get(0);
      service =
          new GatewayService(
              config,
              new AuthenticationService(convertFrom(config)),
              config.isTlsEnabledWithBroker()
                  ? lookupData.getPulsarServiceUrlTls()
                  : lookupData.getPulsarServiceUrl(),
              config.isTlsEnabledWithBroker()
                  ? lookupData.getWebServiceUrlTls()
                  : lookupData.getWebServiceUrl());
    } else {
      service = new GatewayService(config, new AuthenticationService(convertFrom(config)));
    }
    service.start(false);
  }

  @Override
  public Map<InetSocketAddress, ChannelInitializer<SocketChannel>> newChannelInitializers() {
    return service.newChannelInitializers();
  }

  @SneakyThrows
  @Override
  public void close() {
    if (service != null) {
      service.close();
    }
  }
}
