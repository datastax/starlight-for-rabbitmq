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

import com.datastax.oss.starlight.rabbitmq.metadata.VirtualHostMetadata;
import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;

public class TopicExchangeUpdater extends LeaderSelectorListenerAdapter implements Closeable {

  private final GatewayService service;
  private final LeaderSelector leaderSelector;

  public TopicExchangeUpdater(GatewayService service, CuratorFramework client) {
    this.service = service;
    leaderSelector = new LeaderSelector(client, "/topic_exchange_updater", this);
    leaderSelector.autoRequeue();
  }

  public void start() throws IOException {
    leaderSelector.start();
  }

  @Override
  public void close() throws IOException {
    leaderSelector.close();
  }

  @Override
  public void takeLeadership(CuratorFramework client) throws Exception {
    while (true) {
      Thread.sleep(TimeUnit.SECONDS.toMillis(60));
      for (VirtualHostMetadata vhost : service.getContextMetadata().model().getVhosts().values()) {
        List<String> topics = service.getPulsarAdmin().namespaces().getTopics(vhost.getNamespace());
        for (String topic : topics) {
          service.updateQueueSubscriptionsToTopicExchanges(topic).get(5, TimeUnit.SECONDS);
        }
      }
    }
  }
}
