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

package org.gennai.gungnir.ql.analysis;

import static org.gennai.gungnir.GungnirConst.*;

import java.util.ArrayList;
import java.util.List;

public class ParseException extends Exception {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private List<ParseError> errors;

  public ParseException(ArrayList<ParseError> errors) {
    super();
    this.errors = errors;
  }

  public ParseException(String message) {
    super(message);
  }

  @Override
  public String getMessage() {
    if (errors != null) {
      StringBuilder sb = new StringBuilder();
      for (ParseError err : errors) {
        sb.append(err.getMessage());
        sb.append('\n');
      }
      return sb.toString();
    } else {
      return super.getMessage();
    }
  }
}
