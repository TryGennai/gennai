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

package org.gennai.gungnir.tuple.serialization;

import java.util.ArrayList;
import java.util.List;

import org.gennai.gungnir.tuple.Struct;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

public class StructSerializer extends Serializer<Struct> {

  @Override
  public void write(Kryo kryo, Output output, Struct struct) {
    kryo.writeObject(output, struct.getFieldNames());
    kryo.writeObject(output, struct.getValues());
  }

  @Override
  @SuppressWarnings("unchecked")
  public Struct read(Kryo kryo, Input input, Class<Struct> type) {
    List<String> fieldNames = kryo.readObject(input, ArrayList.class);
    List<Object> values = kryo.readObject(input, ArrayList.class);
    return new Struct(fieldNames, values);
  }
}
