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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jline.console.history.FileHistory;

import org.gennai.gungnir.client.GungnirClientException;
import org.gennai.gungnir.console.Console.CommandHandler;
import org.gennai.gungnir.thrift.GungnirServerException;

public final class UploadCommandHandler implements CommandHandler {

  private static final Pattern UPLOAD_COMMAND_PATTERN = Pattern
      .compile("^UPLOAD\\s+(?:'|\")?(.+?)(?:'|\")?$", Pattern.CASE_INSENSITIVE);

  private ConsoleContext context;

  @Override
  public void prepare(ConsoleContext context) {
    this.context = context;
  }

  @Override
  public boolean isMatch(String command) {
    return command.toUpperCase().startsWith("UPLOAD");
  }

  @Override
  public void execute(String command) {
    try {
      Matcher matcher = UPLOAD_COMMAND_PATTERN.matcher(command);
      if (matcher.find()) {
        try {
          Path path = Paths.get(matcher.group(1));
          if (Files.exists(path)) {
            context.getStatement().uploadFile(path);
            context.getReader().println("OK");
          } else {
            context.getReader().println("FAILED: Upload file not found");
          }
        } catch (GungnirServerException e) {
          context.getReader().println("FAILED: " + e.getMessage());
        } catch (GungnirClientException e) {
          context.getReader().println("FAILED: " + e.getMessage());
        } finally {
          context.getReader().getHistory().add(command.replace('\n', ' ') + ';');
          ((FileHistory) context.getReader().getHistory()).flush();
        }
      } else {
        context.getReader().println("UPLOAD commnad usage: UPLOAD FILE_PATH");
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
