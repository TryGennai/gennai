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

package org.gennai.gungnir.ql.stream;

import static org.gennai.gungnir.GungnirConst.*;

import org.gennai.gungnir.GungnirTopology;
import org.gennai.gungnir.GungnirTopologyException;
import org.gennai.gungnir.Period;
import org.gennai.gungnir.ql.stream.TupleJoinStreamBuilder.JoinToTupleDeclarer;
import org.gennai.gungnir.topology.GroupFields;
import org.gennai.gungnir.topology.grouping.FieldsGrouping;
import org.gennai.gungnir.topology.grouping.Grouping;
import org.gennai.gungnir.topology.grouping.ShuffleGrouping;
import org.gennai.gungnir.topology.operator.EachOperator;
import org.gennai.gungnir.topology.operator.EmitOperator;
import org.gennai.gungnir.topology.operator.FilterGroupOperator;
import org.gennai.gungnir.topology.operator.FilterOperator;
import org.gennai.gungnir.topology.operator.JoinOperator;
import org.gennai.gungnir.topology.operator.LimitOperator;
import org.gennai.gungnir.topology.operator.LimitOperator.LimitType;
import org.gennai.gungnir.topology.operator.Operator;
import org.gennai.gungnir.topology.operator.PartitionOperator;
import org.gennai.gungnir.topology.operator.RenameOperator;
import org.gennai.gungnir.topology.operator.SlideOperator;
import org.gennai.gungnir.topology.operator.SnapshotOperator;
import org.gennai.gungnir.topology.operator.limit.LimitInterval;
import org.gennai.gungnir.topology.operator.slide.SlideLength;
import org.gennai.gungnir.topology.operator.snapshot.SnapshotInterval;
import org.gennai.gungnir.topology.processor.EmitProcessor;
import org.gennai.gungnir.topology.processor.FetchProcessor;
import org.gennai.gungnir.tuple.Condition;
import org.gennai.gungnir.tuple.Field;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.TupleAccessor;

public class SingleStream extends BaseStream {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  public SingleStream(GungnirTopology topology, Operator source) {
    super(topology, source);
  }

  public SingleStream(GungnirTopology topology, Operator source, TupleAccessor[] selector) {
    super(topology, source, selector);
  }

  @Override
  protected SingleStream addOperator(Operator target) {
    getTopology().addOperator(getSource(), target, getSelector());
    return new SingleStream(getTopology(), target, getSelector());
  }

  @Override
  protected SingleStream addOperator(Operator target, TupleAccessor... selector) {
    getTopology().addOperator(getSource(), target, getSelector());
    return new SingleStream(getTopology(), target, selector);
  }

  public SingleStream partition(Grouping grouping) {
    return addOperator(new PartitionOperator(grouping));
  }

  public SingleStream join(FetchProcessor processor, String... toFieldNames)
      throws GungnirTopologyException {
    if (toFieldNames == null || toFieldNames.length == 0) {
      throw new GungnirTopologyException("Join field must have at least one");
    }

    GroupFields groupFields = processor.getGroupFields();
    Grouping grouping = null;
    if (groupFields != null) {
      grouping = new FieldsGrouping(groupFields);
    } else {
      grouping = new ShuffleGrouping();
    }
    return addOperator(new PartitionOperator(grouping))
        .addOperator(new JoinOperator(processor, toFieldNames));
  }

  public SingleStream each(Field... fields) throws GungnirTopologyException {
    if (fields == null || fields.length == 0) {
      throw new GungnirTopologyException("Each expression must have at least one");
    }
    return addOperator(new EachOperator(fields));
  }

  public JoinToTupleDeclarer<SingleStream> join(SingleStream joinStream,
      Condition joinCondition) throws GungnirTopologyException {
    return TupleJoinStreamBuilder.builder(this, getTopology()).join(joinStream, joinCondition);
  }

  public JoinToTupleDeclarer<SingleStream> join(SingleStream joinStream,
      Condition joinCondition, Integer parallelism) throws GungnirTopologyException {
    return TupleJoinStreamBuilder.builder(this, getTopology()).join(joinStream, joinCondition,
        parallelism);
  }

  public SingleStream filter(Condition condition) {
    return addOperator(new FilterOperator(condition));
  }

  public SingleStream filterGroup(Period expire, FieldAccessor stateField,
      Condition... conditions) throws GungnirTopologyException {
    if (conditions == null || conditions.length == 0) {
      throw new GungnirTopologyException("Condition must have at least one");
    }
    return addOperator(new FilterGroupOperator(expire, stateField, conditions));
  }

  public SingleStream emit(EmitProcessor processor, FieldAccessor... outputFields)
      throws GungnirTopologyException {
    if (outputFields == null || outputFields.length == 0) {
      throw new GungnirTopologyException("Output field must have at least one");
    }
    return addOperator(new EmitOperator(processor, outputFields));
  }

  public SingleStream slide(SlideLength slideLength, Field... fields)
      throws GungnirTopologyException {
    return addOperator(new SlideOperator(slideLength, fields));
  }

  public SingleStream snapshot(SnapshotInterval interval, Field... fields)
      throws GungnirTopologyException {
    return addOperator(new SnapshotOperator(interval, fields, null));
  }

  public SingleStream snapshot(SnapshotInterval interval, SnapshotInterval expire, Field... fields)
      throws GungnirTopologyException {
    return addOperator(new SnapshotOperator(interval, fields, expire));
  }

  public SingleStream limit(LimitType type, LimitInterval interval)
      throws GungnirTopologyException {
    return addOperator(new LimitOperator(type, interval));
  }

  public GroupedStream<SingleStream> beginGroupBy(FieldAccessor... groupFields) {
    GroupFields fields = new GroupFields(groupFields);
    SingleStream stream = addOperator(new PartitionOperator(new FieldsGrouping(fields)));
    return new GroupedStream<SingleStream>(getTopology(), stream.getSource(), fields,
        getSelector());
  }

  @Override
  public SingleStream select(TupleAccessor... selector) throws GungnirTopologyException {
    if (getSelector() != null) {
      for (TupleAccessor tuple1 : selector) {
        boolean contains = false;
        for (TupleAccessor tuple2 : getSelector()) {
          if (tuple1.equals(tuple2)) {
            contains = true;
            break;
          }
        }

        if (!contains) {
          StringBuilder sb = new StringBuilder();
          sb.append("Can't select (");
          for (int i = 0; i < selector.length; i++) {
            if (i > 0) {
              sb.append(", ");
            }
            sb.append(selector[i]);
          }
          sb.append(") from (");
          for (int i = 0; i < getSelector().length; i++) {
            if (i > 0) {
              sb.append(", ");
            }
            sb.append(getSelector()[i]);
          }
          sb.append(") on stream");

          throw new GungnirTopologyException(sb.toString());
        }
      }
    }
    return new SingleStream(getTopology(), getSource(), selector);
  }

  @Override
  public SingleStream alias(TupleAccessor alias) {
    return addOperator(new RenameOperator(getSelector()[0], alias), alias);
  }

  @Override
  public SingleStream parallelism(Integer parallelism) {
    if (parallelism != null) {
      getSource().setParallelism(parallelism);
    }
    return this;
  }

  @Override
  public String toString() {
    if (getSelector() == null) {
      return getSource().getName() + " -S->";
    } else {
      StringBuilder sb = new StringBuilder();
      sb.append(getSource().getName());
      sb.append(" -S(");
      for (int i = 0; i < getSelector().length; i++) {
        if (i > 0) {
          sb.append(", ");
        }
        sb.append(getSelector()[i]);
      }
      sb.append(")->");
      return sb.toString();
    }
  }
}
