/**
 * Copyright 2013-2014 Recruit Technologies Co., Ltd. and contributors
 * (see CONTRIBUTORS.md)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  A copy of the
 * License is distributed with this work in the LICENSE.md file.  You may
 * also obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gennai.gungnir.topology.processor.spout;

import java.util.List;

public class TupleAndMessageId {

  private List<Object> values;
  private MessageId messageId;

  public TupleAndMessageId(List<Object> values, MessageId messageId) {
    this.values = values;
    this.messageId = messageId;
  }

  public List<Object> getValues() {
    return values;
  }

  public MessageId getMessageId() {
    return messageId;
  }

  @Override
  public String toString() {
    return "{values=" + values + ", id=" + messageId + "}";
  }
}
