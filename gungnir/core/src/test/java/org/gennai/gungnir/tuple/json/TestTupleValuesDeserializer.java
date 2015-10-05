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

import static org.gennai.gungnir.GungnirConst.*;
import static org.gennai.gungnir.tuple.schema.TupleSchema.FieldTypes.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.gennai.gungnir.tuple.Struct;
import org.gennai.gungnir.tuple.TupleValues;
import org.gennai.gungnir.tuple.schema.Schema;
import org.gennai.gungnir.tuple.schema.TupleSchema;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class TestTupleValuesDeserializer {

  ObjectMapper createMapper(Schema schema) {
    SimpleModule module = new SimpleModule("GungnirModule",
        new Version(GUNGNIR_VERSION[0], GUNGNIR_VERSION[1], GUNGNIR_VERSION[2], null, null, null));
    module.addDeserializer(TupleValues.class, new TupleValuesDeserializer(schema));
    ObjectMapper mapper = new ObjectMapper();
    mapper.registerModule(module);
    mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    return mapper;
  }

  @Test
  public void testDeserializeSimpleJson() {
    TupleSchema schema = new TupleSchema("dummy")
        .field("name", STRING)
        .field("content", STRING)
        .field("pages", INT)
        .field("publication_date", TIMESTAMP("yyyy-MM-dd"))
        .field("last_update", TIMESTAMP);
    ObjectMapper mapper = createMapper(schema);
    TupleValues tupleValues = null;
    String targetString = "{"
        + "name: \"The Elements of Style\","
        + "content: \"Elementary Rules of Usage ...\","
        + "pages: 89,"
        + "publication_date: \"1999-08-02\","
        + "last_update: 1396285384"
        + "}";
    try {
      tupleValues = mapper.readValue(targetString, TupleValues.class);
    } catch (Exception e) {
      fail();
    }
    List<Object> values = tupleValues.getValues();
    assertEquals(5, values.size());
    assertEquals("The Elements of Style", (String) values.get(schema.getFieldIndex("name")));
    assertEquals("Elementary Rules of Usage ...",
        (String) values.get(schema.getFieldIndex("content")));
    assertEquals(new Integer(89), (Integer) values.get(schema.getFieldIndex("pages")));
    Calendar cal = Calendar.getInstance();
    cal.set(1999, 7, 2, 0, 0, 0);
    cal.set(Calendar.MILLISECOND, 0);
    assertEquals(cal.getTime(), (Date) values.get(schema.getFieldIndex("publication_date")));
    assertEquals(new Date(TimeUnit.SECONDS.toMillis(1396285384)),
        (Date) values.get(schema.getFieldIndex("last_update")));
  }

  @Test
  public void testDeserializeSimpleJsonNoType() {
    TupleSchema schema = new TupleSchema("dummy")
        .field("name")
        .field("content")
        .field("pages")
        .field("publication_date")
        .field("last_update");
    ObjectMapper mapper = createMapper(schema);
    TupleValues tupleValues = null;
    String targetString = "{"
        + "name: \"The Elements of Style\","
        + "content: \"Elementary Rules of Usage ...\","
        + "pages: 89,"
        + "publication_date: \"1999-08-02\","
        + "last_update: 1396285384"
        + "}";
    try {
      tupleValues = mapper.readValue(targetString, TupleValues.class);
    } catch (Exception e) {
      fail();
    }
    List<Object> values = tupleValues.getValues();
    assertEquals(5, values.size());
    assertEquals("The Elements of Style", (String) values.get(schema.getFieldIndex("name")));
    assertEquals("Elementary Rules of Usage ...",
        (String) values.get(schema.getFieldIndex("content")));
    assertEquals(new Integer(89), (Integer) values.get(schema.getFieldIndex("pages")));
    assertEquals("1999-08-02", (String) values.get(schema.getFieldIndex("publication_date")));
    assertEquals(new Integer(1396285384),
        (Integer) values.get(schema.getFieldIndex("last_update")));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDeserializeJsonWithListField() {
    TupleSchema schema = new TupleSchema("dummy")
        .field("members", LIST(INT));
    ObjectMapper mapper = createMapper(schema);
    TupleValues tupleValues = null;
    String targetString = "{"
        + "members: [1, 8, 19]"
        + "}";
    try {
      tupleValues = mapper.readValue(targetString, TupleValues.class);
    } catch (Exception e) {
      fail();
    }
    List<Object> values = tupleValues.getValues();
    assertEquals(1, values.size());
    List<Integer> deserializedMemberField =
        (List<Integer>) values.get(schema.getFieldIndex("members"));
    assertEquals(3, deserializedMemberField.size());
    assertEquals(new Integer(1), deserializedMemberField.get(0));
    assertEquals(new Integer(8), deserializedMemberField.get(1));
    assertEquals(new Integer(19), deserializedMemberField.get(2));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDeserializeJsonWithMapField() {
    TupleSchema schema = new TupleSchema("dummy")
        .field("features", MAP(STRING, INT));
    ObjectMapper mapper = createMapper(schema);
    TupleValues tupleValues = null;
    String targetString = "{"
        + "features: { width: 30, length: 10 }"
        + "}";
    try {
      tupleValues = mapper.readValue(targetString, TupleValues.class);
    } catch (Exception e) {
      fail();
    }
    List<Object> values = tupleValues.getValues();
    assertEquals(1, values.size());
    Map<String, Integer> deserializedFeatureField =
        (Map<String, Integer>) values.get(schema.getFieldIndex("features"));
    assertEquals(2, deserializedFeatureField.size());
    assertEquals(new Integer(30), deserializedFeatureField.get("width"));
    assertEquals(new Integer(10), deserializedFeatureField.get("length"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDeserializeJsonWithNumberKeyMapField() {
    TupleSchema schema = new TupleSchema("dummy")
        .field("location", STRING)
        .field("geometry", MAP(DOUBLE, DOUBLE));
    ObjectMapper mapper = createMapper(schema);
    TupleValues tupleValues = null;
    String targetString = "{"
        + "location: \"Tokyo station\","
        + "geometry: { \"35.681563888889\": 35.67832667, \"139.76720972222\": 139.77044378}"
        + "}";
    try {
      tupleValues = mapper.readValue(targetString, TupleValues.class);
    } catch (Exception e) {
      fail();
    }
    List<Object> values = tupleValues.getValues();
    assertEquals(2, values.size());
    assertEquals("Tokyo station", (String) values.get(schema.getFieldIndex("location")));
    Map<Double, Double> deserializedGeometryField =
        (Map<Double, Double>) values.get(schema.getFieldIndex("geometry"));
    assertEquals(2, deserializedGeometryField.size());
    assertEquals(new Double(35.67832667), deserializedGeometryField.get(35.681563888889));
    assertEquals(new Double(139.77044378), deserializedGeometryField.get(139.76720972222));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDeserializeJsonWithListMapField() {
    TupleSchema schema = new TupleSchema("dummy")
        .field("items", LIST(MAP(STRING, INT)));
    ObjectMapper mapper = createMapper(schema);
    TupleValues tupleValues = null;
    String targetString = "{"
        + "items: ["
        + "{ itemid: 1, width: 30, length: 10 },"
        + "{ itemid: 2, width: 89, length: 230 }"
        + "]}";
    try {
      tupleValues = mapper.readValue(targetString, TupleValues.class);
    } catch (Exception e) {
      fail();
    }
    List<Object> values = tupleValues.getValues();
    assertEquals(1, values.size());
    List<Map<String, Integer>> deserializedItemsField =
        (List<Map<String, Integer>>) values.get(schema.getFieldIndex("items"));
    assertEquals(2, deserializedItemsField.size());
    assertEquals(new Integer(1), deserializedItemsField.get(0).get("itemid"));
    assertEquals(new Integer(30), deserializedItemsField.get(0).get("width"));
    assertEquals(new Integer(10), deserializedItemsField.get(0).get("length"));
    assertEquals(new Integer(2), deserializedItemsField.get(1).get("itemid"));
    assertEquals(new Integer(89), deserializedItemsField.get(1).get("width"));
    assertEquals(new Integer(230), deserializedItemsField.get(1).get("length"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDeserializeJsonWithStructField() {
    TupleSchema schema = new TupleSchema("dummy")
        .field("textbook", STRUCT()
            .field("name", STRING)
            .field("pages", INT)
            .field("contents", LIST(STRING)));
    ObjectMapper mapper = createMapper(schema);
    TupleValues tupleValues = null;
    String targetString = "{"
        + "textbook: {"
        + "name: \"The Elements of Style\","
        + "pages: 89,"
        + "contents: ["
        + "\"Introductory\","
        + "\"Elementary Rules of Usage\","
        + "\"Elementary Principles of Composition\","
        + "\"A Few Matters of Form\""
        + "]}}";
    try {
      tupleValues = mapper.readValue(targetString, TupleValues.class);
    } catch (Exception e) {
      fail();
    }
    List<Object> values = tupleValues.getValues();
    assertEquals(1, values.size());
    Struct deserializedTextbookField = (Struct) values.get(schema.getFieldIndex("textbook"));
    assertEquals(3, deserializedTextbookField.getValues().size());
    assertEquals("The Elements of Style",
        (String) deserializedTextbookField.getValueByField("name"));
    assertEquals(new Integer(89), (Integer) deserializedTextbookField.getValueByField("pages"));
    List<String> deserializedListField =
        (List<String>) deserializedTextbookField.getValueByField("contents");
    assertEquals(4, deserializedListField.size());
    assertEquals("Introductory", deserializedListField.get(0));
    assertEquals("Elementary Rules of Usage", deserializedListField.get(1));
    assertEquals("Elementary Principles of Composition", deserializedListField.get(2));
    assertEquals("A Few Matters of Form", deserializedListField.get(3));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testDeserializeJsonWithStructFieldNoType() {
    TupleSchema schema = new TupleSchema("dummy")
        .field("textbook", STRUCT()
            .field("name")
            .field("pages")
            .field("contents"));
    ObjectMapper mapper = createMapper(schema);
    TupleValues tupleValues = null;
    String targetString = "{"
        + "textbook: {"
        + "name: \"The Elements of Style\","
        + "pages: 89,"
        + "contents: ["
        + "\"Introductory\","
        + "\"Elementary Rules of Usage\","
        + "\"Elementary Principles of Composition\","
        + "\"A Few Matters of Form\""
        + "]}}";
    try {
      tupleValues = mapper.readValue(targetString, TupleValues.class);
    } catch (Exception e) {
      fail();
    }
    List<Object> values = tupleValues.getValues();
    assertEquals(1, values.size());
    Struct deserializedTextbookField = (Struct) values.get(schema.getFieldIndex("textbook"));
    assertEquals(3, deserializedTextbookField.getValues().size());
    assertEquals("The Elements of Style",
        (String) deserializedTextbookField.getValueByField("name"));
    assertEquals(new Integer(89), (Integer) deserializedTextbookField.getValueByField("pages"));
    List<String> deserializedListField =
        (List<String>) deserializedTextbookField.getValueByField("contents");
    assertEquals(4, deserializedListField.size());
    assertEquals("Introductory", deserializedListField.get(0));
    assertEquals("Elementary Rules of Usage", deserializedListField.get(1));
    assertEquals("Elementary Principles of Composition", deserializedListField.get(2));
    assertEquals("A Few Matters of Form", deserializedListField.get(3));
  }

  @Test
  public void testDeserializeJsonWithoutField() {
    TupleSchema schema = new TupleSchema("dummy");
    ObjectMapper mapper = createMapper(schema);
    TupleValues tupleValues = null;
    String targetString = "{}";
    try {
      tupleValues = mapper.readValue(targetString, TupleValues.class);
    } catch (Exception e) {
      fail();
    }
    List<Object> values = tupleValues.getValues();
    assertEquals(0, values.size());
  }

  @Test(expected = IOException.class)
  public void testDeserializeJsonVoidString() throws IOException {
    TupleSchema schema = new TupleSchema("dummy");
    ObjectMapper mapper = createMapper(schema);
    String targetString = "";
    TupleValues tupleValues = mapper.readValue(targetString, TupleValues.class);
    List<Object> values = tupleValues.getValues();
    assertEquals(0, values.size());
  }
}
