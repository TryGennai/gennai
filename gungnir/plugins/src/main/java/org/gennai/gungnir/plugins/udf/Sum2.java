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

package org.gennai.gungnir.plugins.udf;

import org.gennai.gungnir.utils.ArithmeticOperationException;
import org.gennai.gungnir.utils.GungnirUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Sum2 {

  private static final Logger LOG = LoggerFactory.getLogger(Sum2.class);

  private Object total = 0L;

  public Object evaluate(Object value) {
    try {
      total = GungnirUtils.addition(total, value);
    } catch (ArithmeticOperationException e) {
      LOG.warn("Failed to added {} + {}", total, value);
    }
    return total;
  }

  public Object exclude(Object value) {
    try {
      total = GungnirUtils.subtraction(total, value);
    } catch (ArithmeticOperationException e) {
      LOG.warn("Failed to subtracted {} - {}", total, value);
    }
    return total;
  }

  public void clear() {
    total = 0L;
  }
}
