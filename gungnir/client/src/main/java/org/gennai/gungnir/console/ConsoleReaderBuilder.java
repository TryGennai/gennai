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

import java.io.File;
import java.io.IOException;

import jline.console.ConsoleReader;
import jline.console.completer.ArgumentCompleter;
import jline.console.completer.ArgumentCompleter.AbstractArgumentDelimiter;
import jline.console.completer.ArgumentCompleter.ArgumentDelimiter;
import jline.console.completer.StringsCompleter;
import jline.console.history.FileHistory;

public class ConsoleReaderBuilder {

  private String prompt;
  private String historyFileName;
  private ArgumentCompleter argumentCompleter;

  public ConsoleReaderBuilder prompt(String prompt) {
    this.prompt = prompt;
    return this;
  }

  public ConsoleReaderBuilder history(String historyFileName) {
    this.historyFileName = historyFileName;
    return this;
  }

  public ConsoleReaderBuilder completer(String... keywords) {
    ArgumentDelimiter delimiter = new AbstractArgumentDelimiter() {

      @Override
      public boolean isDelimiterChar(CharSequence buffer, int pos) {
        char c = buffer.charAt(pos);
        return (Character.isWhitespace(c) || c == '(' || c == ')' || c == '[' || c == ']');
      }
    };

    String[] kws = new String[keywords.length * 2];
    int i = 0;
    for (String keyword : keywords) {
      kws[i++] = keyword;
      kws[i++] = keyword.toLowerCase();
    }
    argumentCompleter =
        new ArgumentCompleter(delimiter, new StringsCompleter(kws), new StringsCompleter(kws));
    argumentCompleter.setStrict(false);
    return this;
  }

  public ConsoleReader build() throws IOException {
    ConsoleReader reader = new ConsoleReader();
    if (prompt != null) {
      reader.setPrompt(prompt);
    }
    if (historyFileName != null) {
      String historyDirectory = System.getProperty("user.home");
      String historyFile = historyDirectory + File.separator + historyFileName;
      reader.setHistory(new FileHistory(new File(historyFile)));
    }
    if (argumentCompleter != null) {
      reader.addCompleter(argumentCompleter);
    }
    return reader;
  }
}
