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

import static org.gennai.gungnir.GungnirConfig.*;

import java.util.concurrent.ConcurrentLinkedQueue;

import jline.console.ConsoleReader;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.client.GungnirClient.Statement;

public class ConsoleContext {

  private GungnirConfig config;
  private Statement statement;
  private String accountId;
  private ConsoleReader reader;
  private int logBufferMax;
  private ConcurrentLinkedQueue<String> logBuffer = new ConcurrentLinkedQueue<String>();

  protected void setConfig(GungnirConfig config) {
    this.config = config;
    logBufferMax = config.getInteger(LOG_BUFFER_MAX);
  }

  public GungnirConfig getConfig() {
    return config;
  }

  protected void setStatement(Statement statement) {
    this.statement = statement;
  }

  public Statement getStatement() {
    return statement;
  }

  protected void setAccountId(String accountId) {
    this.accountId = accountId;
  }

  public String getAccountId() {
    return accountId;
  }

  protected void setReader(ConsoleReader reader) {
    this.reader = reader;
  }

  public ConsoleReader getReader() {
    return reader;
  }

  public void addLogMessage(String msg) {
    if (logBuffer.size() > logBufferMax) {
      logBuffer.poll();
    }
    logBuffer.add(msg);
  }

  public ConcurrentLinkedQueue<String> getLogBuffer() {
    return logBuffer;
  }
}
