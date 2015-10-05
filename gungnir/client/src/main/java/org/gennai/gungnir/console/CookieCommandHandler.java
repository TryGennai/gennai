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
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jline.console.history.FileHistory;

import org.gennai.gungnir.console.Console.CommandHandler;
import org.jboss.netty.handler.codec.http.Cookie;

import com.fasterxml.jackson.databind.ObjectMapper;

public final class CookieCommandHandler implements CommandHandler {

  private static final Pattern COOKIE_COMMAND_PATTERN = Pattern
      .compile("^COOKIE(?:\\s+(CLEAR)|)$", Pattern.CASE_INSENSITIVE);

  private Map<String, Cookie> cookiesMap;
  private ConsoleContext context;
  private ObjectMapper mapper;

  public CookieCommandHandler(Map<String, Cookie> cookiesMap) {
    this.cookiesMap = cookiesMap;
  }

  @Override
  public void prepare(ConsoleContext context) {
    this.context = context;
    this.mapper = new ObjectMapper();
  }

  @Override
  public boolean isMatch(String command) {
    return command.toUpperCase().startsWith("COOKIE");
  }

  @Override
  public void execute(String command) {
    try {
      Matcher matcher = COOKIE_COMMAND_PATTERN.matcher(command);
      if (matcher.find()) {
        String cookieCommand = matcher.group(1);
        if (cookieCommand == null) {
          context.getReader().println(mapper.writeValueAsString(cookiesMap.values()));
        } else if ("CLEAR".equalsIgnoreCase(cookieCommand)) {
          cookiesMap.clear();
          context.getReader().println("OK");
        }
      } else {
        context.getReader().println("COOKIE commnad usage: COOKIE [CLEAR]");
      }

      context.getReader().getHistory().add(command.replace('\n', ' ') + ';');
      ((FileHistory) context.getReader().getHistory()).flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
