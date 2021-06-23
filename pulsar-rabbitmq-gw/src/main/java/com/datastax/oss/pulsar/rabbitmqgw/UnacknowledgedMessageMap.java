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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.client.api.MessageId;
import org.apache.qpid.server.flow.FlowCreditManager;
import org.apache.qpid.server.util.ConnectionScopedRuntimeException;

/**
 * See {@link org.apache.qpid.server.protocol.v0_8.UnacknowledgedMessageMap} and
 * org.apache.qpid.server.protocol.v0_8.UnacknowledgedMessageMapImpl
 */
public class UnacknowledgedMessageMap {

  private static final class MessageConsumerAssociation {
    private final MessageId messageId;
    private final Binding binding;
    private final int size;

    private MessageConsumerAssociation(MessageId messageId, Binding binding, int size) {
      this.messageId = messageId;
      this.binding = binding;
      this.size = size;
    }

    public MessageId getMessageId() {
      return messageId;
    }

    public int getSize() {
      return size;
    }

    public Binding getBinding() {
      return binding;
    }
  }

  private final Map<Long, MessageConsumerAssociation> _map = new ConcurrentHashMap<>();

  private final FlowCreditManager creditManager;

  UnacknowledgedMessageMap(FlowCreditManager creditManager) {
    this.creditManager = creditManager;
  }

  public void collect(
      long deliveryTag, boolean multiple, Map<Long, MessageConsumerAssociation> msgs) {
    if (multiple) {
      collect(deliveryTag, msgs);
    } else {
      final MessageConsumerAssociation messageConsumerAssociation = _map.get(deliveryTag);
      if (messageConsumerAssociation != null) {
        msgs.put(deliveryTag, messageConsumerAssociation);
      }
    }
  }

  private void remove(Collection<Long> msgs) {
    for (Long deliveryTag : msgs) {
      remove(deliveryTag, true);
    }
  }

  public MessageConsumerAssociation remove(long deliveryTag, final boolean restoreCredit) {
    MessageConsumerAssociation entry = _map.remove(deliveryTag);
    if (entry != null) {
      if (restoreCredit) {
        creditManager.restoreCredit(1, entry.getSize());
      }
    }
    return entry;
  }

  public void add(long deliveryTag, MessageId message, final Binding binding, int size) {
    if (_map.put(deliveryTag, new MessageConsumerAssociation(message, binding, size)) != null) {
      throw new ConnectionScopedRuntimeException("Unexpected duplicate delivery tag created");
    }
  }

  public MessageId get(long key) {
    MessageConsumerAssociation association = _map.get(key);
    return association == null ? null : association.getMessageId();
  }

  public Collection<MessageConsumerAssociation> acknowledge(long deliveryTag, boolean multiple) {
    if (multiple) {
      Map<Long, MessageConsumerAssociation> ackedMessageMap = new LinkedHashMap<>();
      collect(deliveryTag, true, ackedMessageMap);
      remove(ackedMessageMap.keySet());
      return ackedMessageMap.values();
    } else {
      final MessageConsumerAssociation association = remove(deliveryTag, true);
      if (association != null) {
        final MessageId messageId = association.getMessageId();
        if (messageId != null) {
          return Collections.singleton(association);
        }
      }
      return Collections.emptySet();
    }
  }

  private void collect(long key, Map<Long, MessageConsumerAssociation> msgs) {
    for (Map.Entry<Long, MessageConsumerAssociation> entry : _map.entrySet()) {
      if (entry.getKey() <= key) {
        msgs.put(entry.getKey(), entry.getValue());
      }
    }
  }
}
