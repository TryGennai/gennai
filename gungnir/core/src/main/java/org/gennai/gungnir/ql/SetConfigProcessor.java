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

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gennai.gungnir.ql.analysis.ParseException;
import org.gennai.gungnir.ql.analysis.SemanticAnalyzeException;
import org.gennai.gungnir.ql.session.StatementEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.parser.ParserException;

import com.fasterxml.jackson.databind.ObjectMapper;

public class SetConfigProcessor implements CommandProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(SetConfigProcessor.class);

  private static final Pattern SET_COMMAND_PATTERN = Pattern.compile(
      "^SET\\s+([a-z,A-z][\\w|\\.]+)(?:\\s*=\\s*(.+)|)$", Pattern.CASE_INSENSITIVE);

  private Yaml yaml;
  private ObjectMapper mapper;

  public SetConfigProcessor() {
    yaml = new Yaml(new SafeConstructor());
    mapper = new ObjectMapper();
  }

  @Override
  public boolean canRun(String command) {
    return SET_COMMAND_PATTERN.matcher(command).find();
  }

  @SuppressWarnings("unchecked")
  @Override
  public String run(StatementEntity statement, String command) throws CommandProcessorException {
    if ("SET".equalsIgnoreCase(command)) {
      try {
        return mapper.writeValueAsString(statement.getConfig());
      } catch (Exception e) {
        LOG.error("Failed to convert json format", e);
        return "NG";
      }
    }

    Matcher matcher = SET_COMMAND_PATTERN.matcher(command);
    if (matcher.find()) {
      String key = matcher.group(1);
      String value = matcher.group(2);
      if (value != null) {
        try {
          statement.getConfig().putAll((Map<String, Object>) yaml.load(key + " : " + value));
        } catch (ParserException e) {
          throw new CommandProcessorException(new ParseException(
              "Invalid value of configuration variable"));
        }
      } else {
        Object v = statement.getConfig().get(key);
        if (v != null) {
          return v.toString();
        } else {
          throw new CommandProcessorException(new SemanticAnalyzeException(
              "'" + key + "' is undefined"));
        }
      }
    } else {
      throw new CommandProcessorException(new ParseException(
          "Invalid value of configuration variable"));
    }
    return "OK";
  }

  @Override
  public SetConfigProcessor clone() {
    return new SetConfigProcessor();
  }
}
