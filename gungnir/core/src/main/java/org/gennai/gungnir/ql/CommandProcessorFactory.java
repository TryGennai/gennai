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

import static org.gennai.gungnir.GungnirConfig.*;

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.ql.session.StatementEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;

public class CommandProcessorFactory {

  private static final Logger LOG = LoggerFactory.getLogger(CommandProcessorFactory.class);

  private List<CommandProcessor> processors;
  private Cache<String, List<CommandProcessor>> processorsCache;

  public CommandProcessorFactory() {
    processors = Lists.newArrayList();
    processors.add(new SetConfigProcessor());
    processors.add(new FileCommandProcessor());
    processors.add(new Driver());

    GungnirConfig config = GungnirManager.getManager().getConfig();

    processorsCache =
        CacheBuilder.newBuilder().maximumSize(config.getInteger(COMMAND_PROCESSOR_CACHE_SIZE))
            .expireAfterAccess(config.getInteger(SESSION_TIMEOUT_SECS), TimeUnit.SECONDS).build();
  }

  public CommandProcessor getProcessor(final StatementEntity statement, final String command) {
    if (command.isEmpty()) {
      return null;
    }

    List<CommandProcessor> cachedProcessors = null;
    try {
      cachedProcessors =
          processorsCache.get(statement.getStatementId(), new Callable<List<CommandProcessor>>() {

            @Override
            public List<CommandProcessor> call() throws Exception {
              List<CommandProcessor> cachedProcessors =
                  Lists.newArrayListWithCapacity(processors.size());
              for (CommandProcessor processor : processors) {
                cachedProcessors.add(processor.clone());
              }
              return cachedProcessors;
            }
          });
    } catch (ExecutionException e) {
      LOG.warn("Failed to get processor", e);
      cachedProcessors = Lists.newArrayListWithCapacity(processors.size());
      for (CommandProcessor processor : processors) {
        cachedProcessors.add(processor.clone());
      }
    }

    for (CommandProcessor processor : cachedProcessors) {
      if (processor.canRun(command)) {
        return processor;
      }
    }

    return null;
  }
}
