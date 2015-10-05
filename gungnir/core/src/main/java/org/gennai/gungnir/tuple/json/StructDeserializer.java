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

import org.gennai.gungnir.tuple.InvalidTupleException;
import org.gennai.gungnir.tuple.Struct;
import org.gennai.gungnir.tuple.schema.StructType;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.type.TypeFactory;

public class StructDeserializer extends JsonDeserializer<Struct> {

  private StructType fieldType;
  private JsonDeserializer<?>[] fieldDesers;

  public StructDeserializer(StructType fieldType, TupleValuesDeserializer deserializer) {
    this.fieldType = fieldType;
    fieldDesers = new JsonDeserializer<?>[fieldType.getFieldCount()];
    for (int i = 0; i < fieldType.getFieldCount(); i++) {
      fieldDesers[i] = deserializer.findTypedValueDeserializer(fieldType.getFieldType(i));
    }
  }

  @Override
  public Struct deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
    JsonToken t = jp.getCurrentToken();
    if (t == JsonToken.START_OBJECT) {
      t = jp.nextToken();
    }

    Struct.Builder builder = Struct.builder(fieldType);
    for (; t == JsonToken.FIELD_NAME; t = jp.nextToken()) {
      jp.nextToken();

      Integer index = fieldType.getFieldIndex(jp.getCurrentName());
      if (index == null) {
        throw ctxt.mappingException("Undefined field '{}'" + jp.getCurrentName());
      }

      JsonDeserializer<?> deserializer = null;
      if (fieldDesers[index] != null) {
        deserializer = fieldDesers[index];
      } else {
        deserializer = ctxt.findRootValueDeserializer(TypeFactory.unknownType());
      }
      builder.put(jp.getCurrentName(), deserializer.deserialize(jp, ctxt));
    }

    try {
      return builder.build();
    } catch (InvalidTupleException e) {
      throw ctxt.mappingException(e.getMessage());
    }
  }
}
