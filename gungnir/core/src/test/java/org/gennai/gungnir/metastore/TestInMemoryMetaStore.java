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

package org.gennai.gungnir.metastore;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.Date;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.GungnirTopology;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.GungnirTopology.TopologyStatus;
import org.gennai.gungnir.tuple.schema.Schema;
import org.gennai.gungnir.tuple.schema.TupleSchema;
import org.junit.Before;
import org.junit.Test;

public class TestInMemoryMetaStore {

  private InMemoryMetaStore metaStore;
  private GungnirConfig config;

  @Before
  public void setup() throws Exception {
    metaStore = new InMemoryMetaStore();
    config = GungnirManager.getManager().getConfig();
  }

  @Test
  public void testInsertUserAccount() throws Exception {
    UserEntity userEntity = new UserEntity("uname");
    metaStore.insertUserAccount(userEntity);

    UserEntity result = metaStore.findUserAccountByName("uname");
    assertThat(result.getId(), is(userEntity.getId()));
    assertThat(result.getName(), is(userEntity.getName()));

    metaStore.deleteUserAccount(result);
  }


  @Test(expected = NotStoredException.class)
  public void testDeleteUserAccount() throws Exception {
    UserEntity userEntity = new UserEntity("uname");

    metaStore.insertUserAccount(userEntity);
    metaStore.deleteUserAccount(userEntity);
    assertThat(metaStore.findUserAccounts().size(), is(0));
    metaStore.findUserAccountByName("uname");
  }

  @Test
  public void testInsertSchema() throws Exception {
    UserEntity owner = new UserEntity();
    owner.setId("uid");
    owner.setName("uname");

    Schema schema = new TupleSchema("gennai-tuple");
    schema.setOwner(owner);
    metaStore.insertSchema(schema);
    Schema result = metaStore.findSchema(schema.getSchemaName(), owner);
    assertThat(result.getId(), is(schema.getId()));
    assertThat(result.getSchemaName(), is(schema.getSchemaName()));

    metaStore.deleteSchema(result);
  }

  @Test
  public void testAddTopologyToSchema() throws Exception {
    UserEntity owner = new UserEntity();
    owner.setId("uid");
    owner.setName("uname");

    Schema schema = new TupleSchema("gennai-tuple");
    schema.setOwner(owner);
    schema.setId("schema-id");
    metaStore.insertSchema(schema);
    assertThat(metaStore.changeSchemaToBusy(schema, "gennai-topology"), is(true));
    assertThat(metaStore.changeSchemaToFree(schema, "gennai-topology"), is(true));

    schema.setCreateTime(new Date(System.currentTimeMillis() - 10000));
    assertThat(metaStore.changeSchemaToBusy(schema, "gennai-topology"), is(false));
    assertThat(metaStore.changeSchemaToFree(schema, "gennai-topology"), is(false));

    metaStore.deleteSchema(schema);
  }

  public void testChangeSchemaNotInMetaStore() throws Exception {
    UserEntity owner = new UserEntity();
    owner.setId("uid");
    owner.setName("uname");

    Schema schema = new TupleSchema("gennai-tuple");
    schema.setOwner(owner);
    schema.setId("schema-id");
    assertThat(metaStore.changeSchemaToBusy(schema, "gennai-topology"), is(false));
  }

  @Test
  public void testInsertAndRetrieveTopology() throws Exception {
    UserEntity owner = new UserEntity();
    owner.setId("uid");
    owner.setName("uname");

    GungnirTopology topology = new GungnirTopology(config, owner);
    metaStore.insertTopology(topology);
    GungnirTopology result = metaStore.findTopologyById(topology.getId());
    assertThat(result.getId(), is(topology.getId()));

    metaStore.deleteTopology(result);
  }

  @Test
  public void testGetTopologiesByCurrentStatus() throws Exception {
    UserEntity owner = new UserEntity();
    owner.setId("uid");
    owner.setName("uname");

    GungnirTopology topology1 = new GungnirTopology(config, owner);
    topology1.setName("topology1");
    topology1.setStatus(TopologyStatus.RUNNING);

    GungnirTopology topology2 = new GungnirTopology(config, owner);
    topology2.setName("topology2");
    topology2.setStatus(TopologyStatus.STOPPED);

    GungnirTopology topology3 = new GungnirTopology(config, owner);
    topology3.setName("topology3");
    topology3.setStatus(TopologyStatus.RUNNING);

    metaStore.insertTopology(topology1);
    metaStore.insertTopology(topology2);
    metaStore.insertTopology(topology3);

    assertThat(metaStore.findTopologies(owner, TopologyStatus.RUNNING).size(), is(2));
    assertThat(metaStore.getTopologyStatus(topology2.getId()), is(TopologyStatus.STOPPED));

    metaStore.deleteTopology(topology1);
    metaStore.deleteTopology(topology2);
    metaStore.deleteTopology(topology3);
  }

  @Test(expected = NotStoredException.class)
  public void testTopologyDeletion() throws Exception {
    UserEntity owner = new UserEntity();
    owner.setId("uid");
    owner.setName("uname");

    GungnirTopology topology = new GungnirTopology(config, owner);
    metaStore.insertTopology(topology);

    assertThat(metaStore.findTopologies(owner).size(), is(1));
    metaStore.deleteTopology(topology);

    assertThat(metaStore.findTopologies(owner).size(), is(0));

    metaStore.findTopologyById(topology.getId());
  }

  @Test
  public void testTopologyStatus() throws Exception {
    UserEntity owner = new UserEntity();
    owner.setId("uid");
    owner.setName("uname");

    GungnirTopology originalTopology = new GungnirTopology(config, owner);
    GungnirTopology updatedTopology = new GungnirTopology(config, owner);

    originalTopology.setStatus(TopologyStatus.STOPPED);
    metaStore.insertTopology(originalTopology);
    String id = originalTopology.getId();

    assertThat(metaStore.findTopologyById(id).getStatus(), is(TopologyStatus.STOPPED));

    updatedTopology.setId(id);
    updatedTopology.setStatus(TopologyStatus.STARTING);
    metaStore.changeTopologyStatus(updatedTopology);
    assertThat(metaStore.findTopologyById(id).getStatus(), is(TopologyStatus.STARTING));

    updatedTopology.setStatus(TopologyStatus.STOPPING);
    metaStore.changeTopologyStatus(updatedTopology);
    assertThat(metaStore.findTopologyById(id).getStatus(), is(TopologyStatus.STARTING));

    metaStore.deleteTopology(originalTopology);
  }

  @Test
  public void testIllegalTopologyStatusChange() throws Exception {
    UserEntity owner = new UserEntity();
    owner.setId("uid");
    owner.setName("uname");

    GungnirTopology originalTopology = new GungnirTopology(config, owner);
    GungnirTopology updatedTopology = new GungnirTopology(config, owner);

    originalTopology.setStatus(TopologyStatus.STOPPED);
    metaStore.insertTopology(originalTopology);
    String id = originalTopology.getId();

    updatedTopology.setId(id);
    updatedTopology.setStatus(TopologyStatus.STOPPING);
    assertThat(metaStore.changeTopologyStatus(updatedTopology), is(false));
    assertThat(metaStore.findTopologyById(id).getStatus(), is(TopologyStatus.STOPPED));

    metaStore.deleteTopology(originalTopology);
  }

  @Test
  public void testForceTopologyStatusChange() throws Exception {
    UserEntity owner = new UserEntity();
    owner.setId("uid");
    owner.setName("uname");

    GungnirTopology originalTopology = new GungnirTopology(config, owner);
    GungnirTopology updatedTopology = new GungnirTopology(config, owner);

    originalTopology.setStatus(TopologyStatus.STOPPED);
    metaStore.insertTopology(originalTopology);
    String id = originalTopology.getId();

    updatedTopology.setId(id);
    updatedTopology.setStatus(TopologyStatus.STOPPING);
    metaStore.changeForcedTopologyStatus(updatedTopology);
    assertThat(metaStore.findTopologyById(id).getStatus(), is(TopologyStatus.STOPPING));

    metaStore.deleteTopology(originalTopology);
  }

  @Test(expected = NotStoredException.class)
  public void testDeleteTopology() throws Exception {
    UserEntity owner = new UserEntity();
    owner.setId("uid");
    owner.setName("uname");

    GungnirTopology topology = new GungnirTopology(config, owner);
    metaStore.insertTopology(topology);
    GungnirTopology result = metaStore.findTopologyById(topology.getId());
    assertThat(result.getId(), is(topology.getId()));

    metaStore.deleteTopology(topology);
    metaStore.findTopologyById(topology.getId());
  }

  @Test(expected = NotStoredException.class)
  public void testTrackingIds() throws Exception {
    String id = metaStore.generateTrackingId();
    assertThat(metaStore.getTrackingNo(id), is(1));
    metaStore.getTrackingNo("false id");
  }
}
