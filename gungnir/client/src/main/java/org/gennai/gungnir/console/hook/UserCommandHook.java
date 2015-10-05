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

package org.gennai.gungnir.console.hook;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gennai.gungnir.client.GungnirClientException;
import org.gennai.gungnir.console.StatementHandler.BaseCommandHook;
import org.gennai.gungnir.thrift.GungnirServerException;

public class UserCommandHook extends BaseCommandHook {

  private static final Pattern STATEMENT_PATTERN = Pattern
      .compile("^(?:CREATE|ALTER)\\s+USER\\s+", Pattern.CASE_INSENSITIVE);
  private static final Pattern IDENTIFIED_BY_PATTERN = Pattern.compile("(?:'|\").+(?:'|\")$");

  @Override
  public boolean isMatch(String command) {
    return STATEMENT_PATTERN.matcher(command).find();
  }

  @Override
  public void execute(String command) {
    try {
      try {
        getContext().getReader().println(getContext().getStatement().execute(command));
      } catch (GungnirServerException e) {
        getContext().getReader().println("FAILED: " + e.getMessage());
      } catch (GungnirClientException e) {
        getContext().getReader().println("FAILED: " + e.getMessage());
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      Matcher matcher = IDENTIFIED_BY_PATTERN.matcher(command);
      if (matcher.find()) {
        writeHistory(matcher.replaceFirst("***"));
      }
    }
  }
}
