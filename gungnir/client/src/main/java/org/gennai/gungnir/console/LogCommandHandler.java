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

package org.gennai.gungnir.console;

import java.io.IOException;
import java.util.Iterator;

import jline.console.history.FileHistory;

import org.gennai.gungnir.console.Console.CommandHandler;

public class LogCommandHandler implements CommandHandler {

  private ConsoleContext context;

  @Override
  public void prepare(ConsoleContext context) {
    this.context = context;
  }

  @Override
  public boolean isMatch(String command) {
    return "LOG".equalsIgnoreCase(command);
  }

  @Override
  public void execute(String command) {
    try {
      for (Iterator<String> it = context.getLogBuffer().iterator(); it.hasNext();) {
        context.getReader().println(it.next());
        it.remove();
      }
      context.getReader().flush();

      context.getReader().getHistory().add(command.replace('\n', ' ') + ';');
      ((FileHistory) context.getReader().getHistory()).flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
