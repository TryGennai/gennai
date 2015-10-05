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

import static java.util.logging.Level.*;

import java.util.logging.Handler;
import java.util.logging.LogRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SLF4JHandler extends Handler {

  @Override
  public void publish(LogRecord record) {
    Logger logger = LoggerFactory.getLogger(record.getLoggerName());
    Throwable throwable = record.getThrown();
    if (record.getLevel().equals(SEVERE)) {
      logger.error(record.getMessage(), throwable);
    } else if (record.getLevel().equals(WARNING)) {
      logger.warn(record.getMessage(), throwable);
    } else if (record.getLevel().equals(INFO) || record.getLevel().equals(CONFIG)) {
      logger.info(record.getMessage(), throwable);
    } else if (record.getLevel().equals(FINE)) {
      logger.debug(record.getMessage(), throwable);
    } else if (record.getLevel().equals(FINER) || record.getLevel().equals(FINEST)) {
      logger.trace(record.getMessage(), throwable);
    }
  }

  @Override
  public void flush() {
  }

  @Override
  public void close() {
  }
}
