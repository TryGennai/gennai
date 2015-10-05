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

import java.io.Serializable;
import java.util.Date;
import java.util.List;

import org.gennai.gungnir.UserEntity;

public interface Schema extends Serializable, Cloneable {

  void setId(String id);

  String getId();

  void setOwner(UserEntity owner);

  UserEntity getOwner();

  void setTopologies(List<String> topologies);

  List<String> getTopologies();

  void setCreateTime(Date createTime);

  Date getCreateTime();

  void setComment(String comment);

  String getComment();

  String getSchemaName();

  int getFieldCount();

  Integer getFieldIndex(String fieldName);

  String getFieldName(int index);

  FieldType getFieldType(int index);

  List<String> getFieldNames();

  String[] getPartitionFields();

  void validate() throws SchemaValidateException;

  Schema clone();
}
