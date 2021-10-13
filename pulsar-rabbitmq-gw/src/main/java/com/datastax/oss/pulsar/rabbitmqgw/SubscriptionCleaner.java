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

import com.datastax.oss.pulsar.rabbitmqgw.metadata.BindingMetadata;
import com.datastax.oss.pulsar.rabbitmqgw.metadata.ContextMetadata;
import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.apache.curator.x.async.modeled.versioned.Versioned;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SubscriptionCleaner extends LeaderSelectorListenerAdapter implements Closeable {
  private static final Logger LOGGER = LoggerFactory.getLogger(SubscriptionCleaner.class);

  private final GatewayService service;
  private final LeaderSelector leaderSelector;

  public SubscriptionCleaner(GatewayService service, CuratorFramework client) {
    this.service = service;
    leaderSelector = new LeaderSelector(client, "/subscription_cleaner", this);
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
  public void takeLeadership(CuratorFramework client) {
    try {
      while (true) {
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        Versioned<ContextMetadata> newContext =
            service.newContextMetadata(service.getContextMetadata());

        AtomicBoolean updateContext = new AtomicBoolean(false);

        newContext
            .model()
            .getVhosts()
            .forEach(
                (s, vhost) ->
                    vhost
                        .getSubscriptions()
                        .forEach(
                            (queueName, queueSubscriptions) -> {
                              Iterator<BindingMetadata> iterator =
                                  queueSubscriptions.values().iterator();
                              while (iterator.hasNext()) {
                                BindingMetadata bindingMetadata = iterator.next();
                                if (hasBindingReachedLastMessage(bindingMetadata)) {
                                  try {
                                    service
                                        .getPulsarAdmin()
                                        .topics()
                                        .deleteSubscription(
                                            bindingMetadata.getTopic(),
                                            bindingMetadata.getSubscription(),
                                            true);
                                  } catch (PulsarAdminException.NotFoundException e) {
                                    if (LOGGER.isDebugEnabled()) {
                                      LOGGER.debug(
                                          "Subscription doesn't exist. It may have already been deleted",
                                          e);
                                    }
                                  } catch (PulsarAdminException e) {
                                    LOGGER.error("Error while deleting subscription", e);
                                    continue;
                                  }
                                  iterator.remove();
                                  updateContext.set(true);
                                }
                              }
                            }));

        if (updateContext.get()) {
          try {
            service.saveContext(newContext).toCompletableFuture().get(5, TimeUnit.SECONDS);
          } catch (ExecutionException e) {
            LOGGER.error("Couldn't save configuration after removing a subscription", e);
          }
        }
      }
    } catch (InterruptedException e) {
      LOGGER.warn("Subscription cleaner was interrupted", e);
      Thread.currentThread().interrupt();
    } catch (TimeoutException e) {
      LOGGER.error("Timed out while saving configuration", e);
    }
  }

  private boolean hasBindingReachedLastMessage(BindingMetadata bindingMetadata) {
    if (bindingMetadata.getLastMessageId() != null) {
      try {
        MessageId lastMessageId = MessageId.fromByteArray(bindingMetadata.getLastMessageId());
        if (MessageId.earliest.equals(lastMessageId)) {
          return true;
        }
        List<Message<byte[]>> messages =
            service
                .getPulsarAdmin()
                .topics()
                .peekMessages(bindingMetadata.getTopic(), bindingMetadata.getSubscription(), 1);

        if (messages.size() > 0) {
          Message<byte[]> message = messages.get(0);
          if (message.getMessageId().compareTo(lastMessageId) <= 0) {
            return true;
          }
        } else {
          return true;
        }
      } catch (PulsarAdminException | IOException e) {
        LOGGER.error(
            "Couldn't get last message for subscription " + bindingMetadata.getSubscription(), e);
        return false;
      }
    }
    return false;
  }
}
