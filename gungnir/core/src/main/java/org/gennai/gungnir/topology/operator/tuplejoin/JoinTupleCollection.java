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

package org.gennai.gungnir.topology.operator.tuplejoin;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.operator.OperatorContext;
import org.gennai.gungnir.topology.processor.ProcessorException;
import org.gennai.gungnir.topology.processor.TtlCacheProcessor;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class JoinTupleCollection implements Cloneable {

  private static final Logger LOG = LoggerFactory.getLogger(JoinTupleCollection.class);

  public interface DispatchHandler {

    void dispatch(List<Object> values);
  }

  private ComplexJoinContext complexContext;
  private TtlCacheProcessor processor;
  private int expireSecs;
  private List<FieldAccessor> outputFields;
  private DispatchHandler dispatchHandler;
  private int seekSize;
  private Map<String, TtlCacheProcessor> processorsMap;
  private int[] outputFieldsIndex;
  private boolean[] isOutputAllFields;

  public JoinTupleCollection(ComplexJoinContext complexContext, TtlCacheProcessor processor,
      int expireSecs, List<FieldAccessor> outputFields, int seekSize) {
    this.complexContext = complexContext;
    this.processor = processor;
    this.expireSecs = expireSecs;
    this.outputFields = outputFields;
    this.seekSize = seekSize;
  }

  private JoinTupleCollection(JoinTupleCollection c) {
    this.complexContext = c.complexContext;
    this.processor = c.processor;
    this.expireSecs = c.expireSecs;
    this.outputFields = c.outputFields;
    this.seekSize = c.seekSize;
    this.processorsMap = c.processorsMap;
  }

  public void setDispatchHandler(DispatchHandler dispatchHandler) {
    this.dispatchHandler = dispatchHandler;
  }

  public void prepare(GungnirConfig config, GungnirContext context,
      OperatorContext operatorContext) {
    if (processorsMap == null) {
      processorsMap = Maps.newLinkedHashMap();
      for (JoinContext joinContext : complexContext.getContexts()) {
        TtlCacheProcessor processorCopy = processor.clone();
        try {
          processorCopy.open(config, context, operatorContext,
              joinContext.getFromTuple().getTupleName(), expireSecs, seekSize);
        } catch (ProcessorException e) {
          LOG.error("Failed to open processor", e);
        }
        processorsMap.put(joinContext.getFromTuple().getTupleName(), processorCopy);
      }
    }
  }

  @SuppressWarnings("unchecked")
  public List<Object> select(List<Object> values) {
    if (outputFields == null) {
      return values;
    }

    if (outputFieldsIndex == null) {
      outputFieldsIndex = complexContext.getFieldsIndex(outputFields);
      isOutputAllFields = new boolean[outputFields.size()];
      for (int i = 0; i < outputFields.size(); i++) {
        if ("*".equals(outputFields.get(i).getFieldName())) {
          isOutputAllFields[i] = true;
        }
      }
    }

    List<Object> selectValues = Lists.newArrayList();
    for (int i = 0; i < outputFieldsIndex.length; i++) {
      if (outputFieldsIndex[i] >= 0) {
        if (isOutputAllFields[i]) {
          selectValues.addAll((List<Object>) values.get(outputFieldsIndex[i]));
        } else {
          selectValues.add(values.get(outputFieldsIndex[i]));
        }
      } else {
        selectValues.add(null);
      }
    }

    return selectValues;
  }

  private void join(List<List<List<Object>>> valuesList, int index, List<Object> values) {
    for (int i = 0; i < valuesList.get(index).size(); i++) {
      List<Object> newValues;
      if (values != null) {
        newValues = Lists.newArrayList(values);
      } else {
        newValues = Lists.newArrayList();
      }
      newValues.addAll(valuesList.get(index).get(i));

      if (index < processorsMap.size() - 1) {
        join(valuesList, index + 1, newValues);
      } else {
        dispatchHandler.dispatch(select(newValues));
      }
    }
  }

  private void join(Object key) {
    boolean entired = true;
    for (TtlCacheProcessor p : processorsMap.values()) {
      if (p.size(key) == 0) {
        entired = false;
        break;
      }
    }

    if (entired) {
      List<List<List<Object>>> valuesList = Lists.newArrayListWithCapacity(processorsMap.size());
      for (TtlCacheProcessor p : processorsMap.values()) {
        valuesList.add(p.take(key));
      }
      join(valuesList, 0, null);
    }
  }

  public void put(GungnirTuple tuple) {
    JoinContext joinContext = complexContext.getContext(tuple.getTupleName());
    Object key = joinContext.getKey(tuple);

    if (LOG.isDebugEnabled()) {
      LOG.debug("key:{}, fields: {}, tuple:{}", key, joinContext.getFields(), tuple);
    }

    try {
      processorsMap.get(tuple.getTupleName()).put(key, joinContext.getValues(tuple));
    } catch (ProcessorException e) {
      LOG.error("Failed to store tuple", e);
    }

    join(key);
  }

  public void cleanup() {
    if (processorsMap != null) {
      for (Iterator<Map.Entry<String, TtlCacheProcessor>> it = processorsMap.entrySet().
          iterator(); it.hasNext();) {
        Map.Entry<String, TtlCacheProcessor> entry = it.next();
        entry.getValue().close();
        it.remove();
      }
      processorsMap = null;
    }
  }

  @Override
  public JoinTupleCollection clone() {
    return new JoinTupleCollection(this);
  }
}
