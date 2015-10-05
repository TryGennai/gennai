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

package org.gennai.gungnir.topology.operator;

import static org.gennai.gungnir.GungnirConst.*;

import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.gennai.gungnir.Period;
import org.gennai.gungnir.topology.ConditionEvaluator;
import org.gennai.gungnir.tuple.Condition;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.Field;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.utils.GungnirUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

@Operator.Description(name = "FILTER_GROUP",
    parameterNames = {"expire", "stateField", "conditions"})
public class FilterGroupOperator extends BaseOperator implements ExecOperator {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(FilterGroupOperator.class);

  private Period expire;
  private FieldAccessor stateField;
  private Condition[] conditions;
  private int expireSecs;
  private int[] keepTimes;

  public FilterGroupOperator(Period expire, FieldAccessor stateField, Condition... conditions) {
    super();
    this.expire = expire;
    this.stateField = stateField;
    this.conditions = conditions;
  }

  public FilterGroupOperator(Period expire, Condition... conditions) {
    this(expire, null, conditions);
  }

  private FilterGroupOperator(FilterGroupOperator c) {
    super(c);
    this.expire = c.expire;
    this.stateField = c.stateField;
    this.conditions = c.conditions;
  }

  @Override
  protected void prepare() {
    expireSecs = expire.toSeconds();
    keepTimes = new int[conditions.length];
  }

  @Override
  public void execute(GungnirTuple tuple) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("execute({} {}) {}", getContext().getTopologyId(), getName(), tuple);
    }

    int now = GungnirUtils.currentTimeSecs();
    for (int i = 0; i < keepTimes.length; i++) {
      if (keepTimes[i] > 0) {
        if (now > keepTimes[i]) {
          keepTimes[i] = 0;
          if (LOG.isDebugEnabled()) {
            LOG.debug("Expired condition {}", conditions[i]);
          }
        }
      }
    }

    for (int i = 0; i < conditions.length; i++) {
      if (ConditionEvaluator.isKeep(conditions[i], tuple)) {
        keepTimes[i] = now + expireSecs;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Keep condition {}", conditions[i]);
        }
      }
    }

    boolean isKeep = true;
    for (int time : keepTimes) {
      if (time == 0) {
        isKeep = false;
        break;
      }
    }

    if (isKeep) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Keep {}", getName());
      }

      if (stateField != null) {
        List<Date> status = Lists.newArrayListWithCapacity(keepTimes.length);
        for (int time : keepTimes) {
          status.add(new Date(TimeUnit.SECONDS.toMillis(time)));
        }
        tuple.getTupleValues().getValues().add(status);
      }

      dispatch(tuple.getTupleValues());

      for (int i = 0; i < keepTimes.length; i++) {
        keepTimes[0] = 0;
      }
    }
  }

  @Override
  public List<Field> getOutputFields() {
    if (stateField != null) {
      List<Field> fields = Lists.newArrayListWithCapacity(2);
      fields.add(new FieldAccessor("*"));
      fields.add(stateField);
      return fields;
    } else {
      return null;
    }
  }

  @Override
  public FilterGroupOperator clone() {
    return new FilterGroupOperator(this);
  }
}
