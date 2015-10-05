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

import java.util.Date;
import java.util.List;

import org.apache.storm.guava.collect.Lists;
import org.gennai.gungnir.UserEntity;

public class FileStat implements Cloneable {

  private String id;
  private String name;
  private int size;
  private long checksum;
  private List<String> topologies;
  private UserEntity owner;
  private Date createTime;
  private String comment;


  public FileStat() {
  }

  private FileStat(FileStat c) {
    this.id = c.id;
    this.name = c.name;
    this.size = c.size;
    this.checksum = c.checksum;
    if (c.topologies != null) {
      this.topologies = Lists.newArrayList(c.topologies);
    }
    this.owner = c.owner.clone();
    this.createTime = c.createTime;
    this.comment = c.comment;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void setSize(int size) {
    this.size = size;
  }

  public int getSize() {
    return size;
  }

  public void setChecksum(long checksum) {
    this.checksum = checksum;
  }

  public long getChecksum() {
    return checksum;
  }

  public void setTopologies(List<String> topologies) {
    this.topologies = topologies;
  }

  public List<String> getTopologies() {
    if (topologies == null) {
      topologies = Lists.newArrayList();
    }
    return topologies;
  }

  public void setOwner(UserEntity owner) {
    this.owner = owner;
  }

  public UserEntity getOwner() {
    return owner;
  }

  public void setCreateTime(Date createTime) {
    this.createTime = createTime;
  }

  public Date getCreateTime() {
    return createTime;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public String getComment() {
    return comment;
  }

  @Override
  public FileStat clone() {
    return new FileStat(this);
  }
}
