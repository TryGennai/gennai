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

package org.gennai.gungnir.tuple.json;

import java.io.IOException;
import java.util.List;

import org.gennai.gungnir.tuple.schema.ListType;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.google.common.collect.Lists;

public class ListDeserializer extends JsonDeserializer<List<Object>> {

  private JsonDeserializer<?> elementDeser;

  public ListDeserializer(ListType fieldType, TupleValuesDeserializer deserializer) {
    elementDeser = deserializer.findTypedValueDeserializer(fieldType.getElementType());
  }

  @Override
  public List<Object> deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
    List<Object> list = Lists.newArrayList();
    while (jp.nextToken() != JsonToken.END_ARRAY) {
      list.add(elementDeser.deserialize(jp, ctxt));
    }
    return list;
  }
}
