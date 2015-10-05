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

package org.gennai.gungnir.utils;

import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;

import scala.collection.JavaConverters;
import scala.collection.convert.Wrappers.MapWrapper;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public final class ScalaConverters {

  private ScalaConverters() {
  }

  public static Class<?>[] getScalaTypes() {
    return new Class<?>[] {scala.collection.Seq.class, scala.collection.immutable.Map.class};
  }

  public static boolean isScalaType(Class<?> type) {
    return type.isArray()
        ? type.getComponentType() == scala.collection.Seq.class || type.getComponentType()
              == scala.collection.immutable.Map.class
        : type == scala.collection.Seq.class || type == scala.collection.immutable.Map.class;
  }

  public static Class<?> asJavaType(Class<?> type) {
    if (type.isArray()) {
      if (type.getComponentType() == scala.collection.Seq.class) {
        return Array.newInstance(List.class, 0).getClass();
      } else if (type.getComponentType() == scala.collection.immutable.Map.class) {
        return Array.newInstance(Map.class, 0).getClass();
      }
    } else {
      if (type == scala.collection.Seq.class) {
        return List.class;
      } else if (type == scala.collection.immutable.Map.class) {
        return Map.class;
      }
    }
    return type;
  }

  public static Object asScala(Object value) {
    if (value instanceof List) {
      return JavaConverters.asScalaBufferConverter((List<?>) value).asScala().toSeq();
    } else if (value instanceof Map) {
      @SuppressWarnings("unchecked")
      Map<Object, Object> map = (Map<Object, Object>) value;
      return JavaConverters.mapAsScalaMapConverter(map).asScala()
          .toMap(scala.Predef.<scala.Tuple2<Object, Object>>conforms());
    }
    return value;
  }

  public static Object asJava(Object value) {
    if (value instanceof scala.collection.Seq) {
      return Lists.newArrayList(JavaConverters
          .asJavaCollectionConverter((scala.collection.Seq<?>) value).asJavaCollection());
    } else if (value instanceof scala.collection.immutable.Map) {
      @SuppressWarnings("unchecked")
      scala.collection.immutable.Map<Object, Object> map =
          (scala.collection.immutable.Map<Object, Object>) value;
      return Maps.newHashMap((MapWrapper<Object, Object>) JavaConverters.mapAsJavaMapConverter(map)
          .asJava());
    }
    return value;
  }
}
