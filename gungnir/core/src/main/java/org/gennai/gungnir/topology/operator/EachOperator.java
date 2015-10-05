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

import java.util.List;

import org.gennai.gungnir.topology.udf.Function;
import org.gennai.gungnir.tuple.Field;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.TupleValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

@Operator.Description(name = "EACH", parameterNames = "fields")
public class EachOperator extends BaseOperator implements ExecOperator {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(EachOperator.class);

  private Field[] fields;

  public EachOperator(Field... fields) {
    super();
    this.fields = fields;
  }

  private EachOperator(EachOperator c) {
    super(c);
    this.fields = new Field[c.fields.length];
    for (int i = 0; i < c.fields.length; i++) {
      if (c.fields[i] instanceof Function<?>) {
        this.fields[i] = ((Function<?>) c.fields[i]).clone();
      } else {
        this.fields[i] = c.fields[i];
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
  }

  @Override
  public void execute(GungnirTuple tuple) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Execute({} {}) {}", getContext().getTopologyId(), getName(), tuple);
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
      } else {
        outputValues.add(field.getValue(tuple));
      }
    }

    TupleValues tupleValues = tuple.getTupleValues();
    tupleValues.setValues(outputValues);

    dispatch(tupleValues);
  }

  @Override
  public List<Field> getOutputFields() {
    return Lists.newArrayList(fields);
  }

  @Override
  public EachOperator clone() {
    return new EachOperator(this);
  }
}
