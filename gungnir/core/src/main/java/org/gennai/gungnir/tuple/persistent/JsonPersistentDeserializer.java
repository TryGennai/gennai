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

package org.gennai.gungnir.tuple.persistent;

import static org.gennai.gungnir.GungnirConst.*;

import java.util.Map;

import org.gennai.gungnir.tuple.TupleValues;
import org.gennai.gungnir.tuple.json.TupleValuesDeserializer;
import org.gennai.gungnir.tuple.schema.Schema;

import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.Maps;

public class JsonPersistentDeserializer extends BasePersistentDeserializer {

  private Map<String, ObjectMapper> mappersMap;

  public JsonPersistentDeserializer() {
    super();
  }

  private JsonPersistentDeserializer(JsonPersistentDeserializer c) {
    super(c);
  }

  @Override
  protected void prepare() {
  }

  @Override
  protected void sync() {
    mappersMap = Maps.newHashMap();
  }

  @Override
  protected TupleValues deserialize(TrackingData trackingData, Schema schema)
      throws DeserializeException {
    ObjectMapper mapper = mappersMap.get(trackingData.getTupleName());
    if (mapper == null) {
      SimpleModule module =
          new SimpleModule("GungnirModule",
              new Version(GUNGNIR_VERSION[0], GUNGNIR_VERSION[1], GUNGNIR_VERSION[2], null, null,
                  null));
      module.addDeserializer(TupleValues.class, new TupleValuesDeserializer(schema));

      mapper = new ObjectMapper();
      mapper.registerModule(module);
      mapper.configure(Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);

      mappersMap.put(trackingData.getTupleName(), mapper);
    }

    TupleValues tupleValues = null;
    try {
      tupleValues = mapper.readValue(trackingData.getContent().toString(), TupleValues.class);
    } catch (Exception e) {
      throw new DeserializeException("Failed to deserialize tracking data "
          + trackingData.getContent(), e);
    }

    return tupleValues;
  }

  @Override
  public JsonPersistentDeserializer clone() {
    return new JsonPersistentDeserializer(this);
  }
}
