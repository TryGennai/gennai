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
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.gennai.gungnir.topology.GroupFields;
import org.gennai.gungnir.topology.operator.slide.SlideLength;
import org.gennai.gungnir.topology.operator.slide.SlideLength.LengthType;
import org.gennai.gungnir.topology.udf.AggregateFunction;
import org.gennai.gungnir.topology.udf.Function;
import org.gennai.gungnir.tuple.Field;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.TupleValues;
import org.gennai.gungnir.tuple.store.InMemoryTupleStore;
import org.gennai.gungnir.tuple.store.Query;
import org.gennai.gungnir.tuple.store.Query.ConditionType;
import org.gennai.gungnir.tuple.store.TupleStore;
import org.gennai.gungnir.utils.GungnirUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

@Operator.Description(name = "SLIDE", parameterNames = {"slideLength", "fields"})
public class SlideOperator extends BaseOperator implements ExecOperator {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(SlideOperator.class);

  private SlideLength slideLength;
  private Field[] fields;
  private int periodSecs;
  private List<FieldAccessor> accessFields;
  private List<String> accessFieldNames;
  private TupleStore tupleStore;

  public SlideOperator(SlideLength slideLength, Field[] fields) {
    super();
    this.slideLength = slideLength;
    this.fields = fields;
  }

  private SlideOperator(SlideOperator c) {
    super(c);
    this.slideLength = c.slideLength;
    this.fields = new Field[c.fields.length];
    for (int i = 0; i < c.fields.length; i++) {
      if (c.fields[i] instanceof Function<?>) {
        this.fields[i] = ((Function<?>) c.fields[i]).clone();
      } else {
        this.fields[i] = c.fields[i];
      }
    }
    this.accessFields = c.accessFields;
    this.accessFieldNames = c.accessFieldNames;
    this.tupleStore = c.tupleStore;
  }

  @Override
  protected void prepare() {
    for (Field field : fields) {
      if (field instanceof Function<?>) {
        ((Function<?>) field).prepare(getConfig(), getContext());
      }
    }

    if (slideLength.getType() == LengthType.TIME) {
      periodSecs = slideLength.getPeriod().toSeconds();
    }

    if (accessFields == null) {
      Set<FieldAccessor> targetFields = Sets.newLinkedHashSet();
      for (Field field : fields) {
        if (field instanceof AggregateFunction<?>) {
          List<FieldAccessor> aggFields = ((AggregateFunction<?>) field).getFields();
          if (aggFields != null) {
            targetFields.addAll(((AggregateFunction<?>) field).getFields());
          }
        }
      }

      accessFields = Lists.newArrayList(targetFields);
      accessFieldNames = Lists.newArrayList();
      for (FieldAccessor field : targetFields) {
        accessFieldNames.add(field.getFieldName());
      }

      if (accessFieldNames.isEmpty()) {
        accessFieldNames.add("_name");
      }
    }

    if (tupleStore == null) {
      tupleStore = new InMemoryTupleStore();
      tupleStore.open(getConfig(), getContext());
    }
  }

  @Override
  public void execute(GungnirTuple tuple) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("execute({} {}) {}", getContext().getTopologyId(), getName(), tuple);
    }

    List<Object> values = Lists.newArrayList();
    if (accessFields.isEmpty()) {
      values.add(tuple.getTupleName());
    } else {
      for (FieldAccessor field : accessFields) {
        values.add(field.getValue(tuple));
      }
    }

    Object keyValue = null;
    Integer timeKeyValue = null;
    GroupFields groupFields = getContext().getGroupFields().get(getName());
    if (groupFields != null) {
      keyValue = groupFields.getValues(tuple);
    } else {
      keyValue = getContext().getComponent().getTopologyContext().getThisTaskIndex();
    }

    if (slideLength.getTimeField() != null) {
      Object value = slideLength.getTimeField().getValue(tuple);
      if (value instanceof Date) {
        timeKeyValue = (int) TimeUnit.MILLISECONDS.toSeconds(((Date) value).getTime());
      }
    }

    if (keyValue != null) {
      List<List<Object>> excludeValues = null;
      if (timeKeyValue != null) {
        tupleStore.put(keyValue, timeKeyValue, values);
        excludeValues = tupleStore.findAndRemove(Query.builder().hashKeyValue(keyValue)
            .timeKeyCondition(ConditionType.LT, timeKeyValue - periodSecs).build());
      } else {
        tupleStore.put(keyValue, GungnirUtils.currentTimeSecs(), values);
        int count = tupleStore.count(Query.builder().hashKeyValue(keyValue).build());
        if (count > slideLength.getCount()) {
          excludeValues = tupleStore.findAndRemove(Query.builder().hashKeyValue(keyValue)
              .limit(count - slideLength.getCount()).build());
        }
      }

      GungnirTuple includeTuple = new GungnirTuple(accessFieldNames,
          new TupleValues(tuple.getTupleName(), values));
      List<GungnirTuple> excludeTuples = null;
      if (excludeValues != null && !excludeValues.isEmpty()) {
        excludeTuples = Lists.newArrayListWithCapacity(excludeValues.size());
        for (List<Object> exclude : excludeValues) {
          excludeTuples.add(new GungnirTuple(accessFieldNames,
              new TupleValues(tuple.getTupleName(), exclude)));
        }
      }

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
        } else if (field instanceof AggregateFunction<?>) {
          Object value = field.getValue(includeTuple);
          if (excludeTuples != null) {
            for (GungnirTuple excludeTuple : excludeTuples) {
              value = ((AggregateFunction<?>) field).exclude(excludeTuple);
            }
          }
          outputValues.add(value);
        }
      }

      TupleValues tupleValues = tuple.getTupleValues();
      tupleValues.setValues(outputValues);

      dispatch(tupleValues);
    }
  }

  @Override
  public List<Field> getOutputFields() {
    return Lists.newArrayList(fields);
  }

  @Override
  protected void cleanup() {
    if (tupleStore.isOpen()) {
      tupleStore.close();
    }
  }

  @Override
  public SlideOperator clone() {
    return new SlideOperator(this);
  }
}
