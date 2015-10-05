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

package org.gennai.gungnir.server.tuplestore;

import static org.gennai.gungnir.GungnirConfig.*;

import java.util.List;
import java.util.Map;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.gennai.gungnir.GungnirConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class RewriteRules {

  private static final Logger LOG = LoggerFactory.getLogger(RewriteRules.class);

  private static final Pattern VARIABLE_PATTERN = Pattern.compile("(?:\\$\\{(.+?)\\}|\\$(\\d+))");

  private static class RewriteRule {

    private Pattern pattern;
    private List<Object> target = Lists.newArrayList();
  }

  private List<RewriteRule> rewriteRules = Lists.newArrayList();

  public RewriteRules(GungnirConfig config) {
    List<VariableHandler> variableHandlers = Lists.newArrayList();
    variableHandlers.add(new UserAccountHandler());

    ReplaceHandler replaceHandler = new ReplaceHandler();

    @SuppressWarnings("unchecked")
    List<Map<String, String>> rules = (List<Map<String, String>>) config.get(REWRITE_RULES);
    if (rules != null) {
      for (Map<String, String> rule : rules) {
        String pattern = rule.get(REWRITE_PATTERN);
        String target = rule.get(REWRITE_TARGET);

        if (pattern != null && target != null) {
          RewriteRule rewriteRule = new RewriteRule();

          rewriteRule.pattern = Pattern.compile(pattern);

          Matcher matcher = VARIABLE_PATTERN.matcher(target);
          int start = 0;
          while (true) {
            if (matcher.find(start)) {
              VariableHandler variableHandler = null;
              if (matcher.group(1) != null) {
                String variable = matcher.group(1);
                for (VariableHandler handler : variableHandlers) {
                  if (handler.isMatch(variable)) {
                    variableHandler = handler.build(variable);
                    break;
                  }
                }
              } else {
                variableHandler = replaceHandler.build(matcher.group(2));
              }

              if (matcher.start() > start) {
                rewriteRule.target.add(target.substring(start, matcher.start()));
              }
              if (variableHandler != null) {
                rewriteRule.target.add(variableHandler);
              } else {
                LOG.error("Invalid rewrite rule. '{}' variable can't be used '{}'",
                    matcher.group(),
                    target);
                rewriteRule = null;
                break;
              }
              start = matcher.end();
            } else {
              break;
            }
          }

          if (rewriteRule != null) {
            if (start < target.length()) {
              rewriteRule.target.add(target.substring(start));
            }

            rewriteRules.add(rewriteRule);

            LOG.debug("Add rewrite rule. pattern:'{}', target:'{}'", pattern, target);
          }
        } else {
          LOG.error("Invalid rewrite rule. pattern:{}, target:{}",
              pattern != null ? "'" + pattern + "'" : "undefined",
              target != null ? "'" + target + "'" : "undefined");
        }
      }
    }
  }

  public String rewrite(String uri) {
    if (!rewriteRules.isEmpty()) {
      for (RewriteRule rewriteRule : rewriteRules) {
        Matcher matcher = rewriteRule.pattern.matcher(uri);
        if (matcher.find()) {
          MatchResult matchResult = matcher.toMatchResult();
          StringBuilder sb = new StringBuilder();
          for (Object p : rewriteRule.target) {
            if (p instanceof VariableHandler) {
              String value = ((VariableHandler) p).getValue(matchResult);
              if (value == null) {
                return null;
              }
              sb.append(((VariableHandler) p).getValue(matchResult));
            } else {
              sb.append(p);
            }
          }

          String rewriteUri = sb.toString();
          if (LOG.isDebugEnabled()) {
            LOG.debug("Rewrite '{}' to '{}'", uri, rewriteUri);
          }

          return rewriteUri;
        }
      }
    }
    return uri;
  }
}
