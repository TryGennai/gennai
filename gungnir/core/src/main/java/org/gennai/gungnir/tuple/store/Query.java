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

package org.gennai.gungnir.tuple.store;

public final class Query {

  public enum ConditionType {
    GT, GE, LT, LE
  }

  public interface HashKeyValueDeclarer {

    OptionalDeclarer hashKeyValue(Object hashKeyValue);
  }

  public interface TimeKeyConditionDeclarer extends BuildDeclarer {

    BuildDeclarer timeKeyCondition(ConditionType timeKeyConditionType, int timeKeyValue);
  }

  public interface OffsetDeclarer extends BuildDeclarer {

    LimitDeclarer offset(int offset);
  }

  public interface LimitDeclarer extends BuildDeclarer {

    OffsetDeclarer limit(int limit);
  }

  public interface BuildDeclarer {

    Query build();
  }

  public interface OptionalDeclarer extends BuildDeclarer {

    BuildDeclarer timeKeyCondition(ConditionType timeKeyConditionType, int timeKeyValue);

    LimitDeclarer offset(int offset);

    OffsetDeclarer limit(int limit);
  }

  public static final class Builder implements HashKeyValueDeclarer, OptionalDeclarer,
      TimeKeyConditionDeclarer, OffsetDeclarer, LimitDeclarer {

    private Object hashKeyValue;
    private ConditionType timeKeyConditionType;
    private Integer timeKeyValue;
    private Integer offset;
    private Integer limit;

    private Builder() {
    }

    @Override
    public OptionalDeclarer hashKeyValue(Object hashKeyValue) {
      this.hashKeyValue = hashKeyValue;
      return this;
    }

    @Override
    public BuildDeclarer timeKeyCondition(ConditionType timeKeyConditionType,
        int timeKeyValue) {
      this.timeKeyConditionType = timeKeyConditionType;
      this.timeKeyValue = timeKeyValue;
      return this;
    }

    @Override
    public LimitDeclarer offset(int offset) {
      this.offset = offset;
      return this;
    }

    @Override
    public OffsetDeclarer limit(int limit) {
      this.limit = limit;
      return this;
    }

    @Override
    public Query build() {
      return new Query(this);
    }
  }

  private Object hashKeyValue;
  private ConditionType timeKeyConditionType;
  private Integer timeKeyValue;
  private Integer offset;
  private Integer limit;

  private Query(Builder builder) {
    this.hashKeyValue = builder.hashKeyValue;
    this.timeKeyConditionType = builder.timeKeyConditionType;
    this.timeKeyValue = builder.timeKeyValue;
    this.offset = builder.offset;
    this.limit = builder.limit;
  }

  public static HashKeyValueDeclarer builder() {
    return new Builder();
  }

  public Object getHashKeyValue() {
    return hashKeyValue;
  }

  public ConditionType getTimeKeyConditionType() {
    return timeKeyConditionType;
  }

  public Integer getTimeKeyValue() {
    return timeKeyValue;
  }

  public Integer getOffset() {
    return offset;
  }

  public Integer getLimit() {
    return limit;
  }
}
