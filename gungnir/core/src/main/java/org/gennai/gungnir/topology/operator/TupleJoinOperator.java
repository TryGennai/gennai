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

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import java.util.List;

import org.gennai.gungnir.GungnirTopologyException;
import org.gennai.gungnir.Period;
import org.gennai.gungnir.topology.operator.tuplejoin.ComplexJoinContext;
import org.gennai.gungnir.topology.operator.tuplejoin.JoinTupleCollection;
import org.gennai.gungnir.topology.operator.tuplejoin.JoinTupleCollection.DispatchHandler;
import org.gennai.gungnir.topology.processor.TtlCacheProcessor;
import org.gennai.gungnir.tuple.Field;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.TupleAccessor;
import org.gennai.gungnir.tuple.TupleValues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

@Operator.Description(name = "TUPLE_JOIN",
    parameterNames = {"complexContext", "processor", "expire", "toTuple", "toFields"})
public class TupleJoinOperator extends BaseOperator implements ExecOperator {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(TupleJoinOperator.class);

  private ComplexJoinContext complexContext;
  private TtlCacheProcessor processor;
  private Period expire;
  private TupleAccessor toTuple;
  private List<FieldAccessor> toFields;
  private transient JoinTupleCollection collection;

  // TODO metrics
  public TupleJoinOperator(ComplexJoinContext complexContext, TtlCacheProcessor processor,
      Period expire, TupleAccessor toTuple, List<FieldAccessor> toFields) {
    super();
    this.complexContext = complexContext;
    this.processor = processor;
    this.expire = expire;
    this.toTuple = toTuple;
    this.toFields = toFields;
  }

  public TupleJoinOperator(ComplexJoinContext complexContext, TtlCacheProcessor processor,
      Period expire) {
    this(complexContext, processor, expire, null, null);
  }

  private TupleJoinOperator(TupleJoinOperator c) {
    super(c);
    this.complexContext = c.complexContext;
    this.processor = c.processor;
    this.expire = c.expire;
    this.toTuple = c.toTuple;
    this.toFields = c.toFields;
    this.collection = c.collection.clone();
  }

  private class Dispatcher implements DispatchHandler {

    @Override
    public void dispatch(List<Object> values) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("output values: {}, operator:{}", values, getName());
      }

      TupleJoinOperator.this.dispatch(new TupleValues(getToTuple().getTupleName(), values));
    }
  }

  public TupleAccessor getToTuple() {
    if (toTuple != null) {
      return toTuple;
    } else {
      return complexContext.getFromTuple();
    }
  }

  @Override
  protected void prepare() {
    if (collection == null) {
      Integer seekSize = getConfig().getInteger(TUPLEJOIN_SEEK_SIZE + "." + getId());
      if (seekSize == null) {
        seekSize = getConfig().getInteger(TUPLEJOIN_SEEK_SIZE);
      }

      collection = new JoinTupleCollection(complexContext, processor, expire.toSeconds(),
          toFields, seekSize);
      collection.prepare(getConfig(), getContext(), getOperatorContext());
    }

    collection.setDispatchHandler(new Dispatcher());
  }

  @Override
  public void execute(GungnirTuple tuple) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("execute({} {}) {}", getContext().getTopologyId(), getName(), tuple);
    }

    collection.put(tuple);
  }

  @Override
  public List<Field> getOutputFields() throws GungnirTopologyException {
    List<FieldAccessor> outputFields = null;
    if (toFields != null) {
      outputFields = Lists.newArrayList(toFields);
    } else {
      outputFields = complexContext.getOutputFields();
    }

    List<Field> fields = Lists.newArrayList();
    for (FieldAccessor field : outputFields) {
      fields.add(field);
    }

    return fields;
  }

  @Override
  public void cleanup() {
    collection.cleanup();
  }

  @Override
  public TupleJoinOperator clone() {
    return new TupleJoinOperator(this);
  }
}
