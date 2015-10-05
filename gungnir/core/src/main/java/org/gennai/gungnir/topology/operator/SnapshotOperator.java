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

import java.text.ParseException;
import java.util.Date;
import java.util.List;

import org.gennai.gungnir.topology.operator.snapshot.SnapshotInterval;
import org.gennai.gungnir.topology.operator.snapshot.SnapshotInterval.IntervalType;
import org.gennai.gungnir.topology.udf.AggregateFunction;
import org.gennai.gungnir.topology.udf.Function;
import org.gennai.gungnir.tuple.Field;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.TupleValues;
import org.gennai.gungnir.utils.GungnirUtils;
import org.gennai.gungnir.utils.SnapshotJob;
import org.gennai.gungnir.utils.SnapshotJob.SnapshotTask;
import org.quartz.CronExpression;
import org.quartz.SchedulerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

@Operator.Description(name = "SNAPSHOT", parameterNames = {"interval", "fields", "expire"})
public class SnapshotOperator extends BaseOperator implements ExecOperator {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(SnapshotOperator.class);

  private SnapshotInterval interval;
  private Field[] fields;
  private SnapshotInterval expire;
  private SnapshotJob snapshotJob;
  private TupleValues lastTupleValues;
  private long expireTime;
  private Integer counter;

  public SnapshotOperator(SnapshotInterval interval, Field[] fields, SnapshotInterval expire) {
    super();
    this.interval = interval;
    this.fields = fields;
    this.expire = expire;
  }

  private SnapshotOperator(SnapshotOperator c) {
    super(c);
    this.interval = c.interval;
    this.fields = new Field[c.fields.length];
    for (int i = 0; i < c.fields.length; i++) {
      if (c.fields[i] instanceof Function<?>) {
        this.fields[i] = ((Function<?>) c.fields[i]).clone();
      } else {
        this.fields[i] = c.fields[i];
      }
    }
    this.expire = c.expire;
    this.snapshotJob = c.snapshotJob;
  }

  private class CommitTask implements SnapshotTask {

    @Override
    public void execute() {
      synchronized (SnapshotOperator.this) {
        if (lastTupleValues != null) {
          SnapshotOperator.this.dispatch(lastTupleValues);
          lastTupleValues = null;
        }
      }
    }
  }

  @Override
  protected void prepare() {
    for (Field field : fields) {
      if (field instanceof Function<?>) {
        ((Function<?>) field).prepare(getConfig(), getContext());
      }
    }

    if (interval.getType() == IntervalType.COUNT) {
      counter = 0;
    } else {
      if (snapshotJob == null) {
        snapshotJob = new SnapshotJob();
        try {
          if (interval.getType() == IntervalType.CRON) {
            getContext().getComponent().getShapshotTimer()
                .cronSchedule(interval.getSchedulingPattern(), snapshotJob);
          } else {
            getContext().getComponent().getShapshotTimer()
                .periodSchedule(interval.getPeriod(), snapshotJob);
          }
        } catch (SchedulerException e) {
          LOG.error("Failed to add schedule", e);
        }
      }

      snapshotJob.addTask(new CommitTask());

      if (expire != null) {
        if (interval.getType() == IntervalType.CRON) {
          CronExpression cronExpr;
          try {
            cronExpr = new CronExpression(expire.getSchedulingPattern());
            expireTime = cronExpr.getNextValidTimeAfter(new Date(GungnirUtils.currentTimeMillis()))
                .getTime();
          } catch (ParseException e) {
            LOG.error("Failed to parse pattern", e);
          }
        } else {
          expireTime = GungnirUtils.currentTimeMillis()
              + expire.getPeriod().getTimeUnit().toMillis(expire.getPeriod().getTime());
        }
      }
    }
  }

  private TupleValues getTupleValues(GungnirTuple tuple) {
    List<Object> outputValues = Lists.newArrayList();

    for (Field field : fields) {
      if (field instanceof FieldAccessor) {
        FieldAccessor f = (FieldAccessor) field;
        if (f.isWildcardField()) {
          if (f.getTupleAccessor() == null) {
            outputValues.addAll(tuple.getTupleValues().getValues());
          } else {
            if (tuple.getTupleName().equals(f.getTupleAccessor().getTupleName())) {
              outputValues.addAll(tuple.getTupleValues().getValues());
            }
          }
        } else if (f.isContextField()) {
          outputValues.add(getContext().get(f.getOriginalName()));
        } else {
          outputValues.add(field.getValue(tuple));
        }
      } else {
        outputValues.add(field.getValue(tuple));
      }
    }

    TupleValues tupleValues = tuple.getTupleValues();
    tupleValues.setValues(outputValues);

    return tupleValues;
  }

  private void clear() {
    for (Field field : fields) {
      if (field instanceof AggregateFunction<?>) {
        ((AggregateFunction<?>) field).clear();
      }
    }
  }

  private void expire() {
    if (expire == null) {
      clear();
    } else {
      if (expireTime > 0 && GungnirUtils.currentTimeMillis() >= expireTime) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("expire {} >= {}", GungnirUtils.currentTimeMillis(), expireTime);
        }
        clear();

        if (expire.getType() == IntervalType.CRON) {
          try {
            CronExpression cronExpr = new CronExpression(expire.getSchedulingPattern());
            expireTime = cronExpr.getNextValidTimeAfter(
                new Date(GungnirUtils.currentTimeMillis())).getTime();
          } catch (ParseException e) {
            expireTime = 0;
            LOG.error("Failed to parse pattern", e);
          }
        } else {
          long expireMs = expire.getPeriod().getTimeUnit().toMillis(expire.getPeriod().getTime());
          expireTime +=
              ((GungnirUtils.currentTimeMillis() - expireTime) / expireMs + 1) * expireMs;
        }
      }
    }
  }

  @Override
  public void execute(GungnirTuple tuple) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("execute({} {}) {}", getContext().getTopologyId(), getName(), tuple);
    }

    if (interval.getType() == IntervalType.COUNT) {
      TupleValues tupleValues = getTupleValues(tuple);

      counter++;
      if (counter >= interval.getCount()) {
        dispatch(tupleValues);
        for (Field field : fields) {
          if (field instanceof AggregateFunction<?>) {
            ((AggregateFunction<?>) field).clear();
          }
        }
        counter = 0;
      }
    } else {
      synchronized (this) {
        if (lastTupleValues == null) {
          expire();
        }
        lastTupleValues = getTupleValues(tuple);
      }
    }
  }

  @Override
  public List<Field> getOutputFields() {
    return Lists.newArrayList(fields);
  }

  @Override
  public SnapshotOperator clone() {
    return new SnapshotOperator(this);
  }
}
