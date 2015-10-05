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

package org.gennai.gungnir.ql.task;

import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.GungnirTopology;
import org.gennai.gungnir.GungnirTopologyException;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.metastore.NotStoredException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExplainTask implements Task {

  private static final Logger LOG = LoggerFactory.getLogger(ExplainTask.class);

  private GungnirTopology topology;
  private String topologyName;
  private UserEntity owner;
  private boolean extended;

  public ExplainTask(GungnirTopology topology, boolean extended) {
    this.topology = topology;
    this.extended = extended;
  }

  public ExplainTask(String topologyName, UserEntity owner, boolean extended) {
    this.topologyName = topologyName;
    this.owner = owner;
    this.extended = extended;
  }

  @Override
  public String execute() throws TaskExecuteException {
    try {
      if (topology == null) {
        topology =
            GungnirManager.getManager().getMetaStore().findTopologyByName(topologyName, owner);
      }

      String explain = topology.explain(extended);
      LOG.info(explain);
      return explain;
    } catch (MetaStoreException e) {
      throw new TaskExecuteException(e);
    } catch (NotStoredException e) {
      throw new TaskExecuteException(e);
    } catch (GungnirTopologyException e) {
      throw new TaskExecuteException(e);
    }
  }
}
