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

import org.gennai.gungnir.topology.operator.limit.LimitInterval;
import org.gennai.gungnir.topology.operator.limit.LimitInterval.IntervalType;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.TupleValues;
import org.gennai.gungnir.utils.GungnirUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Operator.Description(name = "LIMIT", parameterNames = {"type", "interval"})
public class LimitOperator extends BaseOperator implements ExecOperator {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(LimitOperator.class);

  public enum LimitType {
    FIRST, LAST
  }

  private LimitType type;
  private LimitInterval interval;
  private int expireSecs;
  private int limitTime;
  private TupleValues lastTupleValues;
  private Integer counter;

  public LimitOperator(LimitType type, LimitInterval interval) {
    super();
    this.type = type;
    this.interval = interval;
  }

  private LimitOperator(LimitOperator c) {
    super(c);
    this.type = c.type;
    this.interval = c.interval;
  }

  @Override
  protected void prepare() {
    if (interval.getType() == IntervalType.TIME) {
      expireSecs = interval.getPeriod().toSeconds();
    } else {
      counter = 0;
    }
  }

  @Override
  public void execute(GungnirTuple tuple) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("execute({} {}) {}", getContext().getTopologyId(), getName(), tuple);
    }

    if (expireSecs > 0) {
      int now = GungnirUtils.currentTimeSecs();
      if (limitTime == 0) {
        if (type == LimitType.FIRST) {
          dispatch(tuple.getTupleValues());
        }
        limitTime = now + expireSecs;
      } else {
        if (now > limitTime) {
          if (type == LimitType.FIRST) {
            dispatch(tuple.getTupleValues());
          } else {
            dispatch(lastTupleValues);
          }
          limitTime = now + expireSecs;
        }
      }
      lastTupleValues = tuple.getTupleValues();
    } else {
      if (counter == 0) {
        if (type == LimitType.FIRST) {
          dispatch(tuple.getTupleValues());
        }
      }
      counter++;
      if (counter >= interval.getCount()) {
        if (type == LimitType.LAST) {
          dispatch(tuple.getTupleValues());
        }
        counter = 0;
      }
    }
  }

  @Override
  public LimitOperator clone() {
    return new LimitOperator(this);
  }
}
