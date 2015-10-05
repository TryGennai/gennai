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

package org.gennai.gungnir.utils.kafka;

import static org.gennai.gungnir.GungnirConst.*;

public class InvalidConfigException extends Exception {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  public InvalidConfigException() {
    super();
  }

  public InvalidConfigException(String message) {
    super(message);
  }

  public InvalidConfigException(Throwable cause) {
    super(cause);
  }

  public InvalidConfigException(String message, Throwable cause) {
    super(message, cause);
  }
}
