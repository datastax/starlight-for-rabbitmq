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
package com.datastax.oss.starlight.rabbitmq.metadata;

import java.util.HashSet;
import java.util.Set;

public class BindingMetadata {
  private String exchange;
  private String topic;
  private String subscription;
  private Set<String> keys = new HashSet<>();
  private byte[] lastMessageId;

  public BindingMetadata() {}

  public BindingMetadata(String exchange, String topic, String subscription) {
    this.exchange = exchange;
    this.topic = topic;
    this.subscription = subscription;
  }

  public String getTopic() {
    return topic;
  }

  public void setTopic(String topic) {
    this.topic = topic;
  }

  public String getSubscription() {
    return subscription;
  }

  public void setSubscription(String subscription) {
    this.subscription = subscription;
  }

  public Set<String> getKeys() {
    return keys;
  }

  public void setKeys(Set<String> keys) {
    this.keys = keys;
  }

  public byte[] getLastMessageId() {
    return lastMessageId;
  }

  public void setLastMessageId(byte[] lastMessageId) {
    this.lastMessageId = lastMessageId;
  }

  public String getExchange() {
    return exchange;
  }

  public void setExchange(String exchange) {
    this.exchange = exchange;
  }
}
