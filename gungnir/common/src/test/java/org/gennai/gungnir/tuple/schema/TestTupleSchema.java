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

package org.gennai.gungnir.tuple.schema;

import static org.gennai.gungnir.tuple.schema.TupleSchema.FieldTypes.*;
import static org.junit.Assert.*;

import org.junit.Test;

public class TestTupleSchema {
  @Test
  public void testCreateTupleSchema() {
    TupleSchema schema = new TupleSchema("dummy");
    schema.field("title", STRING);
    schema.field("content", STRING);
    schema.field("pages", INT);
    assertEquals(schema.getFieldCount(), 3);
    assertEquals(schema.getFieldName(0), "title");
    assertEquals(schema.getFieldName(1), "content");
    assertEquals(schema.getFieldName(2), "pages");
    assertEquals(schema.getFieldType(0), STRING);
  }

  @Test
  public void testPartitionFieldSchema() throws SchemaValidateException {
    TupleSchema schema = new TupleSchema("dummy")
        .field("title", STRING)
        .field("content", STRING)
        .field("pages", INT)
        .partitioned("title", "content");
    schema.validate();
  }

  @Test(expected = SchemaValidateException.class)
  public void testInvalidPartitionFieldSchema() throws SchemaValidateException {
    TupleSchema schema = new TupleSchema("dummy")
        .field("title", STRING)
        .field("content", STRING)
        .field("pages", INT)
        .partitioned("foobar");
    schema.validate();
  }
}
