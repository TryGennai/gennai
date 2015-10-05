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

package org.gennai.gungnir;

import static org.gennai.gungnir.GungnirConst.*;

import java.util.List;
import java.util.Map;

import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.operator.OperatorContext;
import org.gennai.gungnir.topology.processor.EmitProcessor;
import org.gennai.gungnir.topology.processor.ProcessorException;
import org.gennai.gungnir.tuple.GungnirTuple;
import org.gennai.gungnir.tuple.TupleValues;

public class MockEmitProcessor implements EmitProcessor {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private String operatorName;
  private Map<String, List<String>> outputFieldNames;
  private EmitMonitor monitor;

  public MockEmitProcessor(String operatorName, EmitMonitor monitor) {
    this.operatorName = operatorName;
    this.monitor = monitor;
  }

  @Override
  public void open(GungnirConfig config, GungnirContext context, OperatorContext operatorContext,
      Map<String, List<String>> outputFieldNames) throws ProcessorException {
    this.outputFieldNames = outputFieldNames;
  }

  @Override
  public void write(List<TupleValues> tuples) throws ProcessorException {
    for (TupleValues tupleValues : tuples) {
      monitor.emitted(operatorName,
          new GungnirTuple(outputFieldNames.get(tupleValues.getTupleName()), tupleValues));
    }
  }

  @Override
  public void close() {
  }
}
