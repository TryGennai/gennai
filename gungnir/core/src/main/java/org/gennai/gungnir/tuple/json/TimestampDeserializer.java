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
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.gennai.gungnir.tuple.schema.TimestampType;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;

public class TimestampDeserializer extends JsonDeserializer<Date> {

  private ThreadLocal<SimpleDateFormat> formatThreadLocal;

  public TimestampDeserializer(final TimestampType fieldType) {
    if (fieldType.getDateFormat() != null) {
      formatThreadLocal = new ThreadLocal<SimpleDateFormat>() {
        @Override
        protected SimpleDateFormat initialValue() {
          return new SimpleDateFormat(fieldType.getDateFormat());
        }
      };
    }
  }

  @Override
  public Date deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
    JsonToken t = jp.getCurrentToken();
    if (t == JsonToken.VALUE_NUMBER_INT) {
      long time = TimeUnit.SECONDS.toMillis(jp.getLongValue());
      return new Date(time);
    } else if (t == JsonToken.VALUE_STRING) {
      String buff = jp.getText().trim();
      if (buff.isEmpty()) {
        throw ctxt.mappingException("Timestamp field is empty");
      }
      if (formatThreadLocal == null) {
        long time = TimeUnit.SECONDS.toMillis(jp.getLongValue());
        return new Date(time);
      } else {
        try {
          return formatThreadLocal.get().parse(buff);
        } catch (ParseException e) {
          throw ctxt.mappingException("Can't deserialize timestamp field '" + buff + "'");
        }
      }
    }
    throw ctxt.mappingException("Timestamp field is unsupported token " + t);
  }
}
