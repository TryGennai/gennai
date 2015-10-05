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

import java.util.List;
import java.util.Map;

import org.apache.storm.guava.collect.Lists;
import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.metastore.MetaStore;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.tuple.schema.Schema;

import com.google.common.collect.Maps;

public class SchemaRegistry implements Cloneable {

  private Map<String, Schema> schemasMap = Maps.newHashMap();
  private MetaStore metaStore;

  public SchemaRegistry() {
  }

  public SchemaRegistry(SchemaRegistry c) {
    this.schemasMap = Maps.newHashMap(c.schemasMap);
  }

  public void register(Schema schema) {
    schemasMap.put(schema.getSchemaName(), schema);
  }

  public void registerAll(List<Schema> schemas) {
    for (Schema schema : schemas) {
      register(schema);
    }
  }

  public void register(String aliasName, String schemaName) {
    Schema schema = schemasMap.get(schemaName);
    if (schema != null) {
      schemasMap.put(aliasName, schema);
    }
  }

  public void load(UserEntity owner) throws MetaStoreException {
    if (metaStore == null) {
      metaStore = GungnirManager.getManager().getMetaStore();
    }
    schemasMap.clear();
    registerAll(metaStore.findSchemas(owner));
  }

  public boolean exists(String schemaName) {
    return schemasMap.containsKey(schemaName);
  }

  public Schema get(String schemaName) {
    return schemasMap.get(schemaName);
  }

  public boolean isEmpty() {
    return schemasMap.isEmpty();
  }

  public List<Schema> getSchemas() {
    return Lists.newArrayList(schemasMap.values());
  }

  public void unregister(String schemaName) {
    schemasMap.remove(schemaName);
  }

  @Override
  public SchemaRegistry clone() {
    return new SchemaRegistry(this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (String schemaName : schemasMap.keySet()) {
      if (sb.length() > 0) {
        sb.append(", ");
      }
      sb.append(schemaName);
    }
    return sb.toString();
  }
}
