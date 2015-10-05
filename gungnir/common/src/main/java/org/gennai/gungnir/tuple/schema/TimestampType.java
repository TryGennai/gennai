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

import static org.gennai.gungnir.GungnirConst.*;

import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimestampType implements FieldType {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private volatile ThreadLocal<SimpleDateFormat> dateFormatThreadLocal;

  private String dateFormat;

  public TimestampType() {
  }

  public TimestampType(String dateFormat) {
    this.dateFormat = dateFormat;
  }

  public String getDateFormat() {
    return dateFormat;
  }

  @Override
  public String getName() {
    return TypeDef.TIMESTAMP.name();
  }

  @Override
  public Type getJavaType() {
    return TypeDef.TIMESTAMP.getJavaType();
  }

  @Override
  public boolean isInstance(Object obj) {
    if (dateFormat == null) {
      if (!(obj instanceof Date)) {
        return false;
      }
    } else {
      if (dateFormatThreadLocal == null) {
        synchronized (this) {
          if (dateFormatThreadLocal == null) {
            dateFormatThreadLocal = new ThreadLocal<SimpleDateFormat>() {
              @Override
              protected SimpleDateFormat initialValue() {
                return new SimpleDateFormat(dateFormat);
              }
            };
          }
        }
      }
      SimpleDateFormat sdf = dateFormatThreadLocal.get();
      try {
        sdf.parse(obj.toString());
      } catch (ParseException e) {
        return false;
      }
    }
    return true;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((dateFormat == null) ? 0 : dateFormat.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    TimestampType other = (TimestampType) obj;
    if (dateFormat == null) {
      if (other.dateFormat != null) {
        return false;
      }
    } else if (!dateFormat.equals(other.dateFormat)) {
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    if (dateFormat == null) {
      return getName();
    } else {
      return getName() + "(" + dateFormat + ")";
    }
  }
}
