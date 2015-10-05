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

package org.gennai.gungnir.ql;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gennai.gungnir.ql.analysis.ParseException;
import org.gennai.gungnir.ql.session.StatementEntity;
import org.gennai.gungnir.ql.task.DescFileTask;
import org.gennai.gungnir.ql.task.DropFileTask;
import org.gennai.gungnir.ql.task.ShowFilesTask;
import org.gennai.gungnir.ql.task.Task;
import org.gennai.gungnir.ql.task.TaskExecuteException;

public class FileCommandProcessor implements CommandProcessor {

  private static final Pattern FILE_COMMAND_PATTERN = Pattern.compile(
      "^(?:(?:DESC|DROP)\\s+FILE\\s+(.+)|SHOW FILES)$", Pattern.CASE_INSENSITIVE);

  @Override
  public boolean canRun(String command) {
    return FILE_COMMAND_PATTERN.matcher(command).find();
  }

  @Override
  public String run(StatementEntity statement, String command) throws CommandProcessorException {
    Matcher matcher = FILE_COMMAND_PATTERN.matcher(command);
    if (matcher.find()) {
      Task task = null;
      if (command.charAt(0) == 'D' || command.charAt(0) == 'd') {
        if (command.charAt(1) == 'E' || command.charAt(1) == 'e') {
          task = new DescFileTask(matcher.group(1), statement.getOwner());
        } else {
          task = new DropFileTask(matcher.group(1), statement.getOwner());
        }
      } else {
        task = new ShowFilesTask(statement.getOwner());
      }

      try {
        return task.execute();
      } catch (TaskExecuteException e) {
        if (e.getCause() != null) {
          throw new CommandProcessorException(e.getCause());
        } else {
          throw new CommandProcessorException(e);
        }
      }
    } else {
      throw new CommandProcessorException(new ParseException(
          "Invalid value of configuration variable"));
    }
  }

  @Override
  public FileCommandProcessor clone() {
    return new FileCommandProcessor();
  }
}
