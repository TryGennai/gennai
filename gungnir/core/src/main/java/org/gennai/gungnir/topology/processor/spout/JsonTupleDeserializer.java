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

import static org.gennai.gungnir.GungnirConst.*;

import java.util.List;

import org.gennai.gungnir.tuple.TupleValues;
import org.gennai.gungnir.tuple.json.TupleValuesDeserializer;
import org.gennai.gungnir.tuple.schema.Schema;
import org.python.jline.internal.Log;

import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class JsonTupleDeserializer implements TupleDeserializer {

  private ObjectMapper mapper;

  public JsonTupleDeserializer(Schema schema) {
    SimpleModule module =
        new SimpleModule("GungnirModule",
            new Version(GUNGNIR_VERSION[0], GUNGNIR_VERSION[1], GUNGNIR_VERSION[2], null, null,
                null));
    module.addDeserializer(TupleValues.class, new TupleValuesDeserializer(schema));

    mapper = new ObjectMapper();
    mapper.registerModule(module);
    mapper.configure(Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
  }

  @Override
  public List<Object> deserialize(byte[] bytes) {
    try {
      TupleValues tupleValues = mapper.readValue(bytes, TupleValues.class);
      return tupleValues.getValues();
    } catch (Exception e) {
      Log.warn("Failed to deserialize tracking data {}", new String(bytes), e);
    }
    return null;
  }
}
