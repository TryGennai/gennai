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

package org.gennai.gungnir.tuple;

import static org.gennai.gungnir.tuple.schema.TupleSchema.FieldTypes.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.Date;

import org.gennai.gungnir.tuple.schema.StructType;
import org.gennai.gungnir.tuple.schema.TupleSchema;
import org.junit.Before;
import org.junit.Test;

import scala.Int;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class TestGungnirTuple {

  private StructType structType;
  private TupleSchema schema;

  @Before
  public void setup() throws Exception {
    structType = STRUCT().field("s1", STRING).field("s2", LIST(STRING));

    schema = new TupleSchema("dummy")
        .field("tiny", TINYINT)
        .field("small", SMALLINT)
        .field("int", INT)
        .field("big", BIGINT)
        .field("float", FLOAT)
        .field("double", DOUBLE)
        .field("boolean", BOOLEAN)
        .field("time1", TIMESTAMP)
        .field("time2", TIMESTAMP("yyyyMMddHHmmss"))
        .field("list", LIST(INT))
        .field("map", MAP(BIGINT, BOOLEAN))
        .field("struct", structType)
        .field("any");
  }

  @Test
  public void testGetValueByField() throws InvalidTupleException {
    GungnirTuple tuple =
        GungnirTuple.builder(new TupleSchema("dummy").field("title").field("content"))
            .put("title", "this is title")
            .put("content", "this is content")
            .build();
    assertEquals("this is title",
        (String) tuple.getValueByField("title"));
    assertEquals("this is content",
        (String) tuple.getValueByField("content"));
  }

  @Test
  public void testGetValueByNonExistField() throws InvalidTupleException {
    GungnirTuple tuple =
        GungnirTuple.builder(new TupleSchema("dummy").field("title").field("content"))
            .put("title", "this is title")
            .put("content", "this is content")
            .build();
    assertNull(tuple.getValueByField("foobar"));
  }

  @Test
  public void testFieldType() throws InvalidTupleException {
    Date now = new Date();

    GungnirTuple tuple =
        GungnirTuple.builder(schema)
            .put("tiny", Byte.MAX_VALUE)
            .put("small", Short.MAX_VALUE)
            .put("int", Integer.MAX_VALUE)
            .put("big", Long.MAX_VALUE)
            .put("float", Float.MAX_VALUE)
            .put("double", Double.MAX_VALUE)
            .put("boolean", true)
            .put("time1", now)
            .put("time2", "20140131235959")
            .put("list", Lists.newArrayList(1, 2, 3))
            .put("map", ImmutableMap.of(Long.MAX_VALUE, true))
            .put("struct", Struct.builder(structType)
                .put("s1", "text")
                .put("s2", Lists.newArrayList("text1", "text2"))
                .build())
            .put("any", "text")
            .build();

    assertThat(
        tuple,
        is(new GungnirTuple(Lists.newArrayList("tiny", "small", "int", "big", "float", "double",
            "boolean", "time1", "time2", "list", "map", "struct", "any"),
            new TupleValues("dummy",
                Lists.<Object>newArrayList(
                    Byte.MAX_VALUE,
                    Short.MAX_VALUE,
                    Integer.MAX_VALUE,
                    Long.MAX_VALUE,
                    Float.MAX_VALUE,
                    Double.MAX_VALUE,
                    true,
                    now,
                    "20140131235959",
                    Lists.newArrayList(1, 2, 3),
                    ImmutableMap.of(Long.MAX_VALUE, true),
                    new Struct(Lists.newArrayList("s1", "s2"), Lists.<Object>newArrayList("text",
                        Lists.newArrayList("text1", "text2"))),
                    "text")))));

    tuple =
        GungnirTuple.builder(schema)
            .put("tiny", Byte.MAX_VALUE)
            .put("int", Integer.MAX_VALUE)
            .put("boolean", true)
            .put("struct", Struct.builder(structType)
                .put("s1", "text")
                .put("s2", Lists.newArrayList("text1", "text2"))
                .build())
            .put("any", 1.2)
            .build();

    assertThat(
        tuple,
        is(new GungnirTuple(Lists.newArrayList("tiny", "small", "int", "big", "float", "double",
            "boolean", "time1", "time2", "list", "map", "struct", "any"),
            new TupleValues("dummy",
                Lists.<Object>newArrayList(
                    Byte.MAX_VALUE,
                    null,
                    Integer.MAX_VALUE,
                    null,
                    null,
                    null,
                    true,
                    null,
                    null,
                    null,
                    null,
                    new Struct(Lists.newArrayList("s1", "s2"), Lists.<Object>newArrayList("text",
                        Lists.newArrayList("text1", "text2"))),
                    1.2)))));
  }

  @Test(expected = InvalidTupleException.class)
  public void testInvalidTinyIntField() throws InvalidTupleException {
    GungnirTuple.builder(schema)
        .put("tiny", Integer.MAX_VALUE)
        .build();
  }

  @Test(expected = InvalidTupleException.class)
  public void testInvalidSmallIntField() throws InvalidTupleException {
    GungnirTuple.builder(schema)
        .put("small", Integer.MAX_VALUE)
        .build();
  }

  @Test(expected = InvalidTupleException.class)
  public void testInvalidIntField() throws InvalidTupleException {
    GungnirTuple.builder(schema)
        .put("int", Long.MAX_VALUE)
        .build();
  }

  @Test(expected = InvalidTupleException.class)
  public void testInvalidBigIntField() throws InvalidTupleException {
    GungnirTuple.builder(schema)
        .put("big", "text")
        .build();
  }

  @Test(expected = InvalidTupleException.class)
  public void testInvalidFloatField() throws InvalidTupleException {
    GungnirTuple.builder(schema)
        .put("float", Double.MIN_VALUE)
        .build();
  }

  @Test(expected = InvalidTupleException.class)
  public void testInvalidDoubleField() throws InvalidTupleException {
    GungnirTuple.builder(schema)
        .put("double", Float.MIN_VALUE)
        .build();
  }

  @Test(expected = InvalidTupleException.class)
  public void testInvalidBooleanField() throws InvalidTupleException {
    GungnirTuple.builder(schema)
        .put("boolean", Int.MinValue())
        .build();
  }

  @Test(expected = InvalidTupleException.class)
  public void testInvalidTimeField1() throws InvalidTupleException {
    GungnirTuple.builder(schema)
        .put("time1", "20140131235959")
        .build();
  }

  @Test(expected = InvalidTupleException.class)
  public void testInvalidTimeField2() throws InvalidTupleException {
    GungnirTuple.builder(schema)
        .put("time2", new Date())
        .build();
  }

  @Test(expected = InvalidTupleException.class)
  public void testInvalidListField() throws InvalidTupleException {
    GungnirTuple.builder(schema)
        .put("list", Lists.newArrayList("1"))
        .build();
  }

  @Test(expected = InvalidTupleException.class)
  public void testInvalidMapField() throws InvalidTupleException {
    GungnirTuple.builder(schema)
        .put("map", ImmutableMap.of("key", false))
        .build();
  }

  @Test
  public void testStructField() throws InvalidTupleException {
    GungnirTuple.builder(schema)
        .put("struct", Struct.builder(structType).build())
        .build();
  }

  @Test(expected = InvalidTupleException.class)
  public void testInvalidStructField1() throws InvalidTupleException {
    GungnirTuple.builder(schema)
        .put("struct", Struct.builder(structType).put("s1", 1).build())
        .build();
  }

  @Test(expected = InvalidTupleException.class)
  public void testInvalidStructField2() throws InvalidTupleException {
    GungnirTuple
        .builder(schema)
        .put("struct",
            new Struct(Lists.newArrayList("s1", "s2"), Lists.<Object>newArrayList("text", "text")))
        .build();
  }

  @Test(expected = InvalidTupleException.class)
  public void testUndefinedField() throws InvalidTupleException {
    GungnirTuple
        .builder(schema)
        .put("nudef", "text")
        .build();
  }
}
