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
import java.util.Map;

import org.gennai.gungnir.tuple.schema.MapType;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.deser.std.StdKeyDeserializers;
import com.fasterxml.jackson.databind.type.TypeFactory;
import com.google.common.collect.Maps;

public class MapDeserializer extends JsonDeserializer<Map<Object, Object>> {

  private MapType fieldType;
  private volatile KeyDeserializer keyDeser;
  private volatile Boolean keyDeserEnabled;
  private JsonDeserializer<?> valueDeser;

  public MapDeserializer(MapType fieldType, TupleValuesDeserializer deserializer) {
    this.fieldType = fieldType;
    valueDeser = deserializer.findTypedValueDeserializer(fieldType.getValueType());
  }

  @Override
  public Map<Object, Object> deserialize(JsonParser jp, DeserializationContext ctxt)
      throws IOException {
    if (keyDeserEnabled == null) {
      synchronized (this) {
        if (keyDeserEnabled == null) {
          keyDeser =
              StdKeyDeserializers.findStringBasedKeyDeserializer(ctxt.getConfig(), TypeFactory
                  .defaultInstance().constructType(fieldType.getKeyType().getJavaType()));
          keyDeserEnabled = (keyDeser != null);
        }
      }
    }

    JsonToken t = jp.getCurrentToken();
    if (t == JsonToken.START_OBJECT) {
      t = jp.nextToken();
    }

    Map<Object, Object> map = Maps.newLinkedHashMap();
    for (; t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
      jp.nextToken();

      Object key = null;
      if (keyDeserEnabled) {
        key = keyDeser.deserializeKey(jp.getCurrentName(), ctxt);
      } else {
        key = jp.getCurrentName();
      }

      Object value = valueDeser.deserialize(jp, ctxt);

      map.put(key, value);
    }
    return map;
  }
}
