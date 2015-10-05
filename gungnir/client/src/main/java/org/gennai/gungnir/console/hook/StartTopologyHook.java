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

import static org.gennai.gungnir.GungnirConfig.*;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gennai.gungnir.client.GungnirClientException;
import org.gennai.gungnir.console.StatementHandler.BaseCommandHook;
import org.gennai.gungnir.thrift.GungnirServerException;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class StartTopologyHook extends BaseCommandHook {

  private static final Pattern STATEMENT_PATTERN = Pattern
      .compile("^START\\s+TOPOLOGY\\s+(\\w+)$", Pattern.CASE_INSENSITIVE);

  private ObjectMapper mapper = new ObjectMapper();

  @Override
  public boolean isMatch(String command) {
    return STATEMENT_PATTERN.matcher(command).find();
  }

  @Override
  public void execute(String command) {
    try {
      try {
        Matcher matcher = STATEMENT_PATTERN.matcher(command);
        String topologyName = null;
        if (matcher.find()) {
          topologyName = matcher.group(1);
        }

        getContext().getReader().println(getContext().getStatement().execute(command));

        long times = getContext().getConfig().getInteger(TOPOLOGY_STATUS_CHECK_TIMES);
        long interval = getContext().getConfig().getInteger(TOPOLOGY_STATUS_CHECK_INTERVAL);

        getContext().getReader().print("Starting ...");
        getContext().getReader().flush();
        int cnt = 0;
        for (; cnt < times; cnt++) {
          TimeUnit.MILLISECONDS.sleep(interval);

          String res = getContext().getStatement().execute("DESC TOPOLOGY " + topologyName);
          JsonNode descNode = mapper.readTree(res);
          if ("RUNNING".equals(descNode.get("status").asText())) {
            getContext().getReader().println(" Done");
            getContext().getReader().println(res);
            getContext().getReader().flush();
            break;
          } else {
            getContext().getReader().print(".");
            getContext().getReader().flush();
          }
        }
      } catch (GungnirServerException e) {
        getContext().getReader().println("FAILED: " + e.getMessage());
      } catch (GungnirClientException e) {
        getContext().getReader().println("FAILED: " + e.getMessage());
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      } finally {
        writeHistory(command);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
