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

import org.gennai.gungnir.tuple.Struct;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;

public class StructSerializer extends JsonSerializer<Struct> {

  @Override
  public void serialize(Struct value, JsonGenerator jgen, SerializerProvider provider)
      throws IOException {
    jgen.writeStartObject();
    for (int i = 0; i < value.getFieldNames().size(); i++) {
      jgen.writeFieldName(value.getFieldNames().get(i));
      jgen.writeObject(value.getValues().get(i));
    }
    jgen.writeEndObject();
  }
}
