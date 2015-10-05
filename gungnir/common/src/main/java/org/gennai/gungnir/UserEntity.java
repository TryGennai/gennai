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

package org.gennai.gungnir;

import static org.gennai.gungnir.GungnirConst.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;

import org.apache.commons.codec.digest.DigestUtils;

public class UserEntity implements Serializable, Cloneable {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private String id;
  private String name;
  private byte[] password;
  private Date createTime;
  private Date lastModifyTime;

  public UserEntity() {
  }

  public UserEntity(String name) {
    this.name = name;
  }

  public UserEntity(String name, byte[] password) {
    this.name = name;
    this.password = password;
  }

  public UserEntity(String name, String password) {
    this.name = name;
    setPassword(password);
  }

  private UserEntity(UserEntity c) {
    this.id = c.id;
    this.name = c.name;
    this.password = c.password;
    this.createTime = c.createTime;
    this.lastModifyTime = c.lastModifyTime;
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

  public void setPassword(byte[] password) {
    this.password = password;
  }

  public void setPassword(String password) {
    this.password = DigestUtils.sha256(password);
  }

  public byte[] getPassword() {
    return password;
  }

  public void setCreateTime(Date createTime) {
    this.createTime = createTime;
  }

  public Date getCreateTime() {
    return createTime;
  }

  public void setLastModifyTime(Date lastModifyTime) {
    this.lastModifyTime = lastModifyTime;
  }

  public Date getLastModifyTime() {
    return lastModifyTime;
  }

  public boolean validatePassword(String password) {
    if (password == null || password.length() == 0) {
      return false;
    }
    return Arrays.equals(this.password, DigestUtils.sha256(password));
  }

  @Override
  public UserEntity clone() {
    return new UserEntity(this);
  }

  @Override
  public String toString() {
    return "id=" + id + ", name=" + name + ", createTime=" + createTime
        + ", lastModifyTime=" + lastModifyTime;
  }
}
