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

package org.gennai.gungnir.tuple.schema;

import static org.gennai.gungnir.GungnirConst.*;

import java.util.Date;
import java.util.List;

import org.gennai.gungnir.UserEntity;

import com.google.common.collect.Lists;

public abstract class BaseSchema implements Schema {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private String id;
  private transient UserEntity owner;
  private transient List<String> topologies;
  private Date createTime;
  private transient String comment;

  protected BaseSchema() {
  }

  protected BaseSchema(BaseSchema c) {
    this.id = c.id;
    this.owner = c.owner.clone();
    if (c.topologies != null) {
      this.topologies = Lists.newArrayList(c.topologies);
    }
    this.createTime = c.createTime;
    this.comment = c.comment;
  }

  @Override
  public void setId(String id) {
    this.id = id;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public void setOwner(UserEntity owner) {
    this.owner = owner;
  }

  @Override
  public UserEntity getOwner() {
    return owner;
  }

  @Override
  public void setTopologies(List<String> topologies) {
    this.topologies = topologies;
  }

  @Override
  public List<String> getTopologies() {
    if (topologies == null) {
      topologies = Lists.newArrayList();
    }
    return topologies;
  }

  @Override
  public void setCreateTime(Date createTime) {
    this.createTime = createTime;
  }

  @Override
  public Date getCreateTime() {
    return createTime;
  }

  @Override
  public void setComment(String comment) {
    this.comment = comment;
  }

  @Override
  public String getComment() {
    return comment;
  }

  @Override
  public abstract BaseSchema clone();
}
