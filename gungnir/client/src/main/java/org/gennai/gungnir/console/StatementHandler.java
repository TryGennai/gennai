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
import java.util.List;

import jline.console.history.FileHistory;

import org.gennai.gungnir.client.GungnirClientException;
import org.gennai.gungnir.console.Console.CommandHandler;
import org.gennai.gungnir.thrift.GungnirServerException;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.google.common.collect.Lists;

public class StatementHandler implements CommandHandler {

  public interface CommandHook {

    void prepare(ConsoleContext context);

    boolean isMatch(String command);

    void execute(String command);
  }

  public abstract static class BaseCommandHook implements CommandHook {

    private ConsoleContext context;

    protected ConsoleContext getContext() {
      return context;
    }

    @Override
    public void prepare(ConsoleContext context) {
      this.context = context;
    }

    protected void writeHistory(String command) {
      try {
        context.getReader().getHistory().add(command.replace('\n', ' ') + ';');
        ((FileHistory) context.getReader().getHistory()).flush();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private ConsoleContext context;
  private List<CommandHook> commandHooks = Lists.newArrayList();
  private ObjectMapper mapper = new ObjectMapper();
  private ObjectWriter writer = mapper.writer(new DefaultPrettyPrinter()
    .withArrayIndenter(DefaultPrettyPrinter.Lf2SpacesIndenter.instance));

  void addHook(CommandHook hook) {
    commandHooks.add(hook);
  }

  @Override
  public void prepare(ConsoleContext context) {
    this.context = context;
    for (CommandHook hook : commandHooks) {
      hook.prepare(context);
    }
  }

  @Override
  public boolean isMatch(String command) {
    return true;
  }

  @Override
  public void execute(String command) {
    CommandHook commandHook = null;
    for (CommandHook hook : commandHooks) {
      if (hook.isMatch(command)) {
        commandHook = hook;
        break;
      }
    }

    if (commandHook == null) {
      try {
        try {
          String res = context.getStatement().execute(command);
          if ((res.startsWith("{") && res.endsWith("}"))
              || (res.startsWith("[") && res.endsWith("]"))) {
            try {
              res = writer.writeValueAsString(mapper.readTree(res));
            } catch (JsonProcessingException ignore) {
              ignore = null;
            }
          }
          context.getReader().println(res);
        } catch (GungnirServerException e) {
          context.getReader().println("FAILED: " + e.getMessage());
        } catch (GungnirClientException e) {
          context.getReader().println("FAILED: " + e.getMessage());
        } finally {
          context.getReader().getHistory().add(command.replace('\n', ' ') + ';');
          ((FileHistory) context.getReader().getHistory()).flush();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      commandHook.execute(command);
    }
  }
}
