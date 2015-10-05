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

import org.gennai.gungnir.topology.processor.FetchProcessor;
import org.gennai.gungnir.topology.processor.ProcessorException;
import org.gennai.gungnir.tuple.Field;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.TupleValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

@Operator.Description(name = "JOIN", parameterNames = {"processor", "toFieldNames"})
public class JoinOperator extends BaseOperator implements ExecOperator {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(JoinOperator.class);

  private FetchProcessor processor;
  private String[] toFieldNames;
  private boolean open = false;

  public JoinOperator(FetchProcessor processor, String[] toFieldNames) {
    super();
    this.processor = processor;
    this.toFieldNames = toFieldNames;
  }

  private JoinOperator(JoinOperator c) {
    super(c);
    this.processor = c.processor;
    this.toFieldNames = c.toFieldNames;
    this.open = c.open;
  }

  @Override
  protected void prepare() {
    if (!open) {
      try {
        processor.open(getConfig(), getContext());
      } catch (ProcessorException e) {
        LOG.error("Failed to open processor", e);
      }
      open = true;
    }
  }

  @Override
  public void execute(GungnirTuple tuple) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("execute({} {}) {}", getContext().getTopologyId(), getName(), tuple);
    }

    try {
      List<List<Object>> valuesList = processor.fetch(tuple);
      for (List<Object> joinValues : valuesList) {
        List<Object> values = Lists.newArrayList(tuple.getTupleValues().getValues());
        values.addAll(joinValues);
        dispatch(new TupleValues(tuple.getTupleName(), values));
      }
    } catch (ProcessorException e) {
      LOG.error("Failed to execute in processor", e);
    }
  }

  @Override
  public List<Field> getOutputFields() {
    List<Field> fields = Lists.newArrayListWithCapacity(toFieldNames.length + 1);
    fields.add(new FieldAccessor("*"));
    for (int i = 0; i < toFieldNames.length; i++) {
      fields.add(new FieldAccessor(toFieldNames[i]));
    }
    return fields;
  }

  @Override
  public void cleanup() {
    processor.close();
  }

  @Override
  public JoinOperator clone() {
    return new JoinOperator(this);
  }
}
