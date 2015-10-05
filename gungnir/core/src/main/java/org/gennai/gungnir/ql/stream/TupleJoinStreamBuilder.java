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

import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;

import org.gennai.gungnir.GungnirTopology;
import org.gennai.gungnir.GungnirTopologyException;
import org.gennai.gungnir.Period;
import org.gennai.gungnir.topology.grouping.JoinKeyGrouping;
import org.gennai.gungnir.topology.operator.PartitionOperator;
import org.gennai.gungnir.topology.operator.TupleJoinOperator;
import org.gennai.gungnir.topology.operator.tuplejoin.ComplexJoinContext;
import org.gennai.gungnir.topology.operator.tuplejoin.ComplexJoinKey;
import org.gennai.gungnir.topology.operator.tuplejoin.JoinContext;
import org.gennai.gungnir.topology.operator.tuplejoin.JoinKey;
import org.gennai.gungnir.topology.operator.tuplejoin.SimpleJoinContext;
import org.gennai.gungnir.topology.operator.tuplejoin.SimpleJoinKey;
import org.gennai.gungnir.topology.processor.TtlCacheProcessor;
import org.gennai.gungnir.tuple.ComplexCondition;
import org.gennai.gungnir.tuple.Condition;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.SimpleCondition;
import org.gennai.gungnir.tuple.TupleAccessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public final class TupleJoinStreamBuilder<T extends Stream> {

  private static final Logger LOG = LoggerFactory.getLogger(TupleJoinStreamBuilder.class);

  private static class JoinTuple {

    private TupleAccessor fromTuple;
    private Condition condition;
    private Integer parallelism;

    JoinTuple(TupleAccessor fromTuple, Condition condition, Integer parallelism) {
      this.fromTuple = fromTuple;
      this.condition = condition;
      this.parallelism = parallelism;
    }
  }

  private TupleJoinStreamBuilder() {
  }

  public interface JoinStreamDeclarer<T extends Stream> {

    JoinToTupleDeclarer<T> join(T joinStream, Condition joinCondition)
        throws GungnirTopologyException;

    JoinToTupleDeclarer<T> join(T joinStream, Condition joinCondition, Integer parallelism)
        throws GungnirTopologyException;
  }

  public interface JoinToTupleDeclarer<T extends Stream> extends JoinStreamDeclarer<T> {

    JoinTupleExpireDeclarer<T> to(TupleAccessor toTuple, FieldAccessor... toFields)
        throws GungnirTopologyException;
  }

  public interface JoinTupleExpireDeclarer<T extends Stream> {

    BuildDeclarer<T> expire(Period expire);
  }

  public interface BuildDeclarer<T extends Stream> {

    BuildDeclarer<T> using(TtlCacheProcessor processor);

    T build() throws GungnirTopologyException;
  }

  public static final class Builder<T extends Stream> implements JoinStreamDeclarer<T>,
      JoinToTupleDeclarer<T>, JoinTupleExpireDeclarer<T>, BuildDeclarer<T> {

    private T fromStream;
    private GungnirTopology topology;
    private List<T> joinStreams;
    private List<JoinTuple> joinTuples;
    private TupleAccessor toTuple;
    private FieldAccessor[] toFields;
    private Period expire;
    private TtlCacheProcessor processor;

    private Builder(T fromStream, GungnirTopology topology) throws GungnirTopologyException {
      if (fromStream.getSelector().length != 1) {
        throw new GungnirTopologyException("Joined stream must be selected one tuple");
      }
      this.fromStream = fromStream;
      this.topology = topology;
      this.joinStreams = Lists.newArrayList();
      this.joinTuples = Lists.newArrayList();
    }

    @Override
    public JoinToTupleDeclarer<T> join(T joinStream, Condition joinCondition)
        throws GungnirTopologyException {
      join(joinStream, joinCondition, 0);
      return this;
    }

    @Override
    public JoinToTupleDeclarer<T> join(T joinStream, Condition joinCondition, Integer parallelism)
        throws GungnirTopologyException {
      if (joinStream.getSelector().length == 1) {
        joinStreams.add(joinStream);
        joinTuples.add(new JoinTuple(joinStream.getSelector()[0], joinCondition, parallelism));
      } else {
        throw new GungnirTopologyException("Stream that passes through multiple tuples can't join");
      }
      return this;
    }

    @Override
    public JoinTupleExpireDeclarer<T> to(TupleAccessor toTuple, FieldAccessor... toFields)
        throws GungnirTopologyException {
      this.toTuple = toTuple;
      this.toFields = toFields;
      return this;
    }

    @Override
    public BuildDeclarer<T> expire(Period expire) {
      this.expire = expire;
      return this;
    }

    @Override
    public BuildDeclarer<T> using(TtlCacheProcessor processor) {
      this.processor = processor;
      return this;
    }

    private JoinKey[] toJoinKey(Condition condition) throws GungnirTopologyException {
      if (condition instanceof ComplexCondition) {
        ComplexJoinKey[] joinKeys = new ComplexJoinKey[2];
        joinKeys[0] = new ComplexJoinKey();
        joinKeys[1] = new ComplexJoinKey();
        for (Condition c : ((ComplexCondition) condition).getConditions()) {
          if (c instanceof SimpleCondition) {
            JoinKey[] simpleKeys = toJoinKey(c);
            joinKeys[0].add((SimpleJoinKey) simpleKeys[0]);
            joinKeys[1].add((SimpleJoinKey) simpleKeys[1]);
          } else {
            throw new GungnirTopologyException("Invalid join condition");
          }
        }
        return joinKeys;
      } else {
        SimpleCondition simpleCondition = (SimpleCondition) condition;
        if (simpleCondition.getField() instanceof FieldAccessor
            && simpleCondition.getValue() instanceof FieldAccessor) {
          if (simpleCondition.getType() == SimpleCondition.Type.EQ) {
            SimpleJoinKey[] joinKeys = new SimpleJoinKey[2];
            joinKeys[0] = new SimpleJoinKey((FieldAccessor) simpleCondition.getField());
            joinKeys[1] = new SimpleJoinKey((FieldAccessor) simpleCondition.getValue());
            return joinKeys;
          }
        }
        throw new GungnirTopologyException("Invalid join condition");
      }
    }

    private List<FieldAccessor> getCollectionFields(TupleAccessor tuple, FieldAccessor[] toFields) {
      List<FieldAccessor> fields = Lists.newArrayList();
      for (int i = 0; i < toFields.length; i++) {
        if (toFields[i].getTupleAccessor().getTupleName().equals(tuple.getTupleName())) {
          fields.add(toFields[i]);
        }
      }
      return fields;
    }

    @Override
    @SuppressWarnings("unchecked")
    public T build() throws GungnirTopologyException {
      Map<String, SimpleJoinContext> contextsMap = Maps.newHashMap();

      SimpleJoinContext fromContext = new SimpleJoinContext(fromStream.getSelector()[0],
          getCollectionFields(fromStream.getSelector()[0], toFields));
      contextsMap.put(fromStream.getSelector()[0].getTupleName(), fromContext);

      ComplexJoinContext complexContext = new ComplexJoinContext();

      List<T> streams = Lists.newArrayList();
      streams.add(fromStream);

      int parallelism = 0;

      for (int i = 0; i < joinTuples.size(); i++) {
        SimpleJoinContext simpleContext = new SimpleJoinContext(joinTuples.get(i).fromTuple,
            getCollectionFields(joinTuples.get(i).fromTuple, toFields));
        contextsMap.put(joinTuples.get(i).fromTuple.getTupleName(), simpleContext);

        JoinKey[] joinKeys = toJoinKey(joinTuples.get(i).condition);

        if (joinKeys[0].getTupleName().equals(joinTuples.get(i).fromTuple.getTupleName())) {
          simpleContext.setJoinKey(joinKeys[0]);
        } else {
          simpleContext.setJoinKey(joinKeys[1]);
        }

        if (complexContext.getContexts().isEmpty()) {
          if (joinKeys[0].getTupleName().equals(joinTuples.get(i).fromTuple.getTupleName())) {
            fromContext.setJoinKey(joinKeys[1]);
          } else {
            fromContext.setJoinKey(joinKeys[0]);
          }
          complexContext.addContext(fromContext);
        } else {
          boolean grouping = false;
          for (JoinContext joinContext : complexContext.getContexts()) {
            if (joinContext.getJoinKey().equals(joinKeys[0])
                || joinContext.getJoinKey().equals(joinKeys[1])) {
              grouping = true;
              break;
            }
          }

          if (!grouping) {
            if (joinKeys[0].getTupleName().equals(joinTuples.get(i).fromTuple.getTupleName())) {
              complexContext.setJoinKey(joinKeys[1]);
            } else {
              complexContext.setJoinKey(joinKeys[0]);
            }

            LOG.info("Build tuple join {}", complexContext);

            PartitionOperator partitionOperator = new PartitionOperator(
                new JoinKeyGrouping(complexContext));
            TupleJoinOperator tupleJoinOperator = new TupleJoinOperator(complexContext, processor,
                expire);

            T stream = topology.from(streams.toArray((T[]) Array.newInstance(fromStream.getClass(),
                0)));

            if (stream instanceof SingleStream) {
              stream = (T) ((SingleStream) stream).addOperator(partitionOperator)
                  .addOperator(tupleJoinOperator, complexContext.getFromTuple())
                  .parallelism(parallelism);
            } else {
              stream = (T) ((GroupedStream<?>) stream).addOperator(partitionOperator)
                  .addOperator(tupleJoinOperator, complexContext.getFromTuple())
                  .parallelism(parallelism);
            }

            ComplexJoinContext joinContext = new ComplexJoinContext();
            joinContext.addContext(complexContext);
            complexContext = joinContext;

            streams.clear();
            streams.add(stream);

            parallelism = 0;
          }
        }

        complexContext.addContext(simpleContext);

        streams.add(joinStreams.get(i));

        if (joinTuples.get(i).parallelism != null && joinTuples.get(i).parallelism > parallelism) {
          parallelism = joinTuples.get(i).parallelism;
        }
      }

      LOG.info("Build tuple join {}", complexContext);

      PartitionOperator partitionOperator = new PartitionOperator(
          new JoinKeyGrouping(complexContext));
      TupleJoinOperator tupleJoinOperator =
          new TupleJoinOperator(complexContext, processor, expire,
              toTuple, Lists.newArrayList(toFields));

      T stream = topology.from(streams.toArray((T[]) Array.newInstance(fromStream.getClass(), 0)));

      if (stream instanceof SingleStream) {
        return (T) ((SingleStream) stream).addOperator(partitionOperator)
            .addOperator(tupleJoinOperator, toTuple).parallelism(parallelism);
      } else {
        return (T) ((GroupedStream<?>) stream).addOperator(partitionOperator)
            .addOperator(tupleJoinOperator, toTuple).parallelism(parallelism);
      }
    }
  }

  public static <T extends Stream> JoinStreamDeclarer<T> builder(T fromStream,
      GungnirTopology topology) throws GungnirTopologyException {
    return new Builder<T>(fromStream, topology);
  }
}
