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

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.gennai.gungnir.graph.GroupedStreamEdge;
import org.gennai.gungnir.graph.StreamEdge;
import org.gennai.gungnir.ql.FunctionEntity;
import org.gennai.gungnir.ql.stream.GroupedStream;
import org.gennai.gungnir.ql.stream.SingleStream;
import org.gennai.gungnir.ql.stream.Stream;
import org.gennai.gungnir.topology.GroupFields;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.component.ExecBolt;
import org.gennai.gungnir.topology.component.ExecSpout;
import org.gennai.gungnir.topology.dispatcher.Dispatcher;
import org.gennai.gungnir.topology.dispatcher.FilterDispatcher;
import org.gennai.gungnir.topology.dispatcher.GroupingDispatcher;
import org.gennai.gungnir.topology.dispatcher.MultiDispatcher;
import org.gennai.gungnir.topology.dispatcher.SingleDispatcher;
import org.gennai.gungnir.topology.dispatcher.TupleNameFilter;
import org.gennai.gungnir.topology.grouping.GlobalGrouping;
import org.gennai.gungnir.topology.grouping.Grouping;
import org.gennai.gungnir.topology.grouping.GroupingBuilder;
import org.gennai.gungnir.topology.grouping.ShuffleGrouping;
import org.gennai.gungnir.topology.operator.EachOperator;
import org.gennai.gungnir.topology.operator.MergeOperator;
import org.gennai.gungnir.topology.operator.Operator;
import org.gennai.gungnir.topology.operator.PartitionOperator;
import org.gennai.gungnir.topology.operator.RenameOperator;
import org.gennai.gungnir.topology.operator.SlideOperator;
import org.gennai.gungnir.topology.operator.SnapshotOperator;
import org.gennai.gungnir.topology.operator.SpoutOperator;
import org.gennai.gungnir.topology.operator.TupleJoinOperator;
import org.gennai.gungnir.topology.operator.metrics.Metrics;
import org.gennai.gungnir.topology.operator.metrics.MultiCountMeter;
import org.gennai.gungnir.topology.processor.SpoutProcessor;
import org.gennai.gungnir.topology.udf.UserDefined;
import org.gennai.gungnir.tuple.Field;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.TupleAccessor;
import org.gennai.gungnir.tuple.schema.Schema;
import org.gennai.gungnir.tuple.schema.TupleSchema;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.traverse.BreadthFirstIterator;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.generated.StormTopology;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class GungnirTopology implements Serializable, Cloneable {

  private static final long serialVersionUID = SERIAL_VERSION_UID;
  private static final Logger LOG = LoggerFactory.getLogger(GungnirTopology.class);

  public enum TopologyStatus {
    STARTING, RUNNING, STOPPING, STOPPED
  }

  private GungnirConfig config;
  private DefaultDirectedGraph<Operator, StreamEdge> graph;

  private String id;
  private String name;
  private TopologyStatus status;
  private transient UserEntity owner;
  private Date createTime;
  private transient String comment;

  private transient GungnirContext context;
  private transient ExecSpout spout;
  private transient List<ExecBolt> bolts;
  private transient Map<Grouping, Integer> boltsIndex;
  private transient TopologyBuilder builder;
  private transient List<BoltDeclarer> boltDeclarers;

  public GungnirTopology(GungnirConfig config, UserEntity owner) {
    this.config = config;
    this.owner = owner;
    graph = new DefaultDirectedGraph<Operator, StreamEdge>(StreamEdge.class);
    status = TopologyStatus.STOPPED;
  }

  @SuppressWarnings("unchecked")
  private GungnirTopology(GungnirTopology c) {
    this.config = c.config.clone();
    this.graph = (DefaultDirectedGraph<Operator, StreamEdge>) c.graph.clone();
    this.id = c.id;
    this.name = c.name;
    this.status = c.status;
    this.owner = c.owner.clone();
    this.createTime = c.createTime;
    this.comment = c.comment;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getId() {
    return id;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }

  public void setStatus(TopologyStatus status) {
    this.status = status;
  }

  public TopologyStatus getStatus() {
    return status;
  }

  public void setOwner(UserEntity owner) {
    this.owner = owner;
  }

  public UserEntity getOwner() {
    return owner;
  }

  public GungnirConfig getConfig() {
    return config;
  }

  public void setCreateTime(Date createTime) {
    this.createTime = createTime;
  }

  public Date getCreateTime() {
    return createTime;
  }

  public void setComment(String comment) {
    this.comment = comment;
  }

  public String getComment() {
    return comment;
  }

  private TupleAccessor[] mergeSelector(TupleAccessor[] selector1, TupleAccessor[] selector2) {
    if (selector1 == null || selector2 == null) {
      return null;
    } else {
      Set<TupleAccessor> tuples = Sets.newLinkedHashSet();
      for (TupleAccessor tuple : selector1) {
        tuples.add(tuple);
      }
      for (TupleAccessor tuple : selector2) {
        tuples.add(tuple);
      }
      return tuples.toArray(new TupleAccessor[0]);
    }
  }

  public void addOperator(Operator source, Operator target, TupleAccessor[] selector) {
    if (!graph.containsVertex(target)) {
      graph.addVertex(target);
    }

    if (graph.containsEdge(source, target)) {
      StreamEdge edge = graph.getEdge(source, target);
      edge.setSelector(mergeSelector(edge.getSelector(), selector));
    } else {
      graph.addEdge(source, target, new StreamEdge(selector));
    }
  }

  public void addOperator(Operator source, Operator target, GroupFields groupFields,
      TupleAccessor[] selector) {
    if (!graph.containsVertex(target)) {
      graph.addVertex(target);
    }

    if (graph.containsEdge(source, target)) {
      StreamEdge edge = graph.getEdge(source, target);
      edge.setSelector(mergeSelector(edge.getSelector(), selector));
    } else {
      graph.addEdge(source, target, new GroupedStreamEdge(groupFields, selector));
    }
  }

  public SingleStream from(SpoutProcessor processor, Schema... schemas) {
    SpoutOperator target = new SpoutOperator(processor, schemas);
    graph.addVertex(target);

    return new SingleStream(this, target);
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public <T extends Stream> T from(T... streams) throws GungnirTopologyException {
    if (streams.length == 0) {
      throw new GungnirTopologyException("Streams is empty");
    } else if (streams.length == 1) {
      return streams[0];
    } else {
      if (streams[0] instanceof SingleStream) {
        List<TupleAccessor> tuples = Lists.newArrayList();
        Operator source = streams[0].getSource();
        for (T stream : streams) {
          if (!(stream instanceof SingleStream)) {
            throw new GungnirTopologyException("Can't merge stream and grouped stream "
                + Arrays.toString(streams));
          }

          if (stream.getSelector() != null) {
            for (TupleAccessor s : stream.getSelector()) {
              if (tuples.contains(s)) {
                throw new GungnirTopologyException(
                    "Same tuple pass through more than once the path " + Arrays.toString(streams));
              }
              tuples.add(s);
            }
          } else {
            throw new GungnirTopologyException(
                "Same tuple pass through more than once the path " + Arrays.toString(streams));
          }

          if (source != null && !stream.getSource().equals(source)) {
            source = null;
          }
        }

        // same source
        if (source != null) {
          return (T) new SingleStream(this, source, tuples.toArray(new TupleAccessor[0]));
        }

        MergeOperator target = new MergeOperator();
        for (T stream : streams) {
          addOperator(stream.getSource(), target, stream.getSelector());
        }

        TupleAccessor[] selector = null;
        if (tuples != null && tuples.size() > 0) {
          selector = tuples.toArray(new TupleAccessor[0]);
        }

        return (T) new SingleStream(this, target, selector);
      } else {
        GroupFields groupFields = null;
        List<TupleAccessor> tuples = Lists.newArrayList();
        Operator source = streams[0].getSource();
        for (T stream : streams) {
          if (!(stream instanceof GroupedStream<?>)) {
            throw new GungnirTopologyException("Can't merge stream and grouped stream "
                + Arrays.toString(streams));
          }

          GroupFields g = ((GroupedStream<?>) stream).getGroupFields();
          if (groupFields == null) {
            groupFields = g;
          } else {
            if (!g.equals(groupFields)) {
              throw new GungnirTopologyException("Can't merge stream grouped by different fields "
                  + Arrays.toString(streams));
            }
          }
          if (stream.getSelector() != null) {
            for (TupleAccessor s : stream.getSelector()) {
              if (tuples.contains(s)) {
                throw new GungnirTopologyException(
                    "Same tuple pass through more than once the path " + Arrays.toString(streams));
              }
              tuples.add(s);
            }
          } else {
            throw new GungnirTopologyException("Same tuple pass through more than once the path "
                + Arrays.toString(streams));
          }

          if (source != null && !stream.getSource().equals(source)) {
            source = null;
          }
        }

        MergeOperator target = new MergeOperator();
        for (T stream : streams) {
          addOperator(stream.getSource(), target, stream.getSelector());
        }

        TupleAccessor[] selector = null;
        if (tuples != null && tuples.size() > 0) {
          selector = tuples.toArray(new TupleAccessor[0]);
        }

        return (T) new GroupedStream(this, target, groupFields, selector);
      }
    }
  }

  public List<Schema> getUsedSchemas() {
    List<Schema> schemas = Lists.newArrayList();
    SpoutOperator spoutOperator = null;
    if (graph.vertexSet().size() > 0) {
      spoutOperator = (SpoutOperator) graph.vertexSet().iterator().next();
    }

    if (spoutOperator != null) {
      for (Schema schema : spoutOperator.getSchemas()) {
        if (schema instanceof TupleSchema) {
          // TODO view schema
          schemas.add(schema);
        }
      }
    }

    return schemas;
  }

  public List<FunctionEntity> getUsedFunctions() {
    Map<String, FunctionEntity> functionsMap = Maps.newHashMap();

    BreadthFirstIterator<Operator, StreamEdge> it =
        new BreadthFirstIterator<Operator, StreamEdge>(graph);
    while (it.hasNext()) {
      Operator operator = it.next();

      List<Field> fields = null;
      if (operator instanceof EachOperator) {
        fields = ((EachOperator) operator).getOutputFields();
      } else if (operator instanceof SlideOperator) {
        fields = ((SlideOperator) operator).getOutputFields();
      } else if (operator instanceof SnapshotOperator) {
        fields = ((SnapshotOperator) operator).getOutputFields();
      }

      if (fields != null) {
        for (Field field : fields) {
          if (field instanceof UserDefined) {
            FunctionEntity function = ((UserDefined) field).getFunction();
            if (!functionsMap.containsKey(function.getName())) {
              functionsMap.put(function.getName(), function);
            }
          }
        }
      }
    }

    return Lists.newArrayList(functionsMap.values());
  }

  private Map<String, List<String>> getInputFields(
      DefaultDirectedGraph<Operator, StreamEdge> graph,
      Operator operator, Map<String, Map<String, List<String>>> outputFields) {
    Map<String, List<String>> inFieldsMap = Maps.newLinkedHashMap();
    for (StreamEdge edge : graph.incomingEdgesOf(operator)) {
      Map<String, List<String>> fieldsMap = outputFields.get(graph.getEdgeSource(edge).getName());
      if (edge.getSelector() != null) {
        for (TupleAccessor tuple : edge.getSelector()) {
          inFieldsMap.put(tuple.getTupleName(), fieldsMap.get(tuple.getTupleName()));
        }
      } else {
        inFieldsMap.putAll(fieldsMap);
      }
    }
    return inFieldsMap;
  }

  private Map<String, Map<String, List<String>>> getOutputFields(
      DefaultDirectedGraph<Operator, StreamEdge> graph, List<Operator> operators)
      throws GungnirTopologyException {
    Map<String, Map<String, List<String>>> outputFields = Maps.newLinkedHashMap();

    TopologicalOrderIterator<Operator, StreamEdge> it =
        new TopologicalOrderIterator<Operator, StreamEdge>(graph);
    while (it.hasNext()) {
      Operator operator = it.next();

      if (operator instanceof SpoutOperator) {
        Map<String, List<String>> outFieldsMap = Maps.newLinkedHashMap();
        for (Schema schema : ((SpoutOperator) operator).getSchemas()) {
          outFieldsMap.put(schema.getSchemaName(), Lists.newArrayList(schema.getFieldNames()));
        }
        outputFields.put(operator.getName(), outFieldsMap);
      } else if (operator instanceof TupleJoinOperator) {
        Map<String, List<String>> inFieldsMap = getInputFields(graph, operator, outputFields);

        List<String> fieldNames = Lists.newArrayList();
        List<Field> fields = operator.getOutputFields();
        for (Field field : fields) {
          FieldAccessor f = (FieldAccessor) field;
          while (f.getParentAccessor() != null) {
            f = f.getParentAccessor();
          }

          String tupleName = null;
          if (f.getTupleAccessor() != null) {
            tupleName = f.getTupleAccessor().getTupleName();
          }

          if (f.isWildcardField()) {
            List<String> fnames = inFieldsMap.get(tupleName);
            for (String fname : fnames) {
              if (fieldNames.contains(fname)) {
                throw new GungnirTopologyException(
                    "Field of same field name exists in " + operator.getName());
              }
            }
            fieldNames.addAll(fnames);
          } else {
            if (fieldNames.contains(f.getFieldName())) {
              throw new GungnirTopologyException(
                  "Field of same field name exists in " + operator.getName());
            }
            fieldNames.add(f.getFieldName());
          }
        }

        Map<String, List<String>> outFieldsMap = Maps.newLinkedHashMap();
        String toTupleName = ((TupleJoinOperator) operator).getToTuple().getTupleName();
        outFieldsMap.put(toTupleName, fieldNames);
        outputFields.put(operator.getName(), outFieldsMap);
      } else if (operator instanceof RenameOperator) {
        Map<String, List<String>> inFieldsMap = getInputFields(graph, operator, outputFields);

        String fromTupleName = ((RenameOperator) operator).getFromTuple().getTupleName();
        List<String> fieldNames = inFieldsMap.get(fromTupleName);

        Map<String, List<String>> outFieldsMap = Maps.newLinkedHashMap();
        String toTupleName = ((RenameOperator) operator).getToTuple().getTupleName();
        outFieldsMap.put(toTupleName, fieldNames);
        outputFields.put(operator.getName(), outFieldsMap);
      } else {
        Map<String, List<String>> inFieldsMap = getInputFields(graph, operator, outputFields);

        List<Field> fields = operator.getOutputFields();
        if (fields != null) {
          Map<String, List<String>> outFieldsMap = Maps.newLinkedHashMap();
          for (Map.Entry<String, List<String>> entry : inFieldsMap.entrySet()) {
            List<String> fieldNames = Lists.newArrayList();

            for (Field field : fields) {
              if (field instanceof FieldAccessor) {
                FieldAccessor f = (FieldAccessor) field;
                while (f.getParentAccessor() != null) {
                  f = f.getParentAccessor();
                }

                if (f.getTupleAccessor() != null) {
                  throw new GungnirTopologyException(
                      "Can't specify tuple explicitly in " + operator.getName());
                }

                if (f.isWildcardField()) {
                  List<String> fnames = inFieldsMap.get(entry.getKey());
                  for (String fname : fnames) {
                    if (fieldNames.contains(fname)) {
                      throw new GungnirTopologyException(
                          "Field of same field name exists in " + operator.getName());
                    }
                  }
                  fieldNames.addAll(entry.getValue());
                } else {
                  if (fieldNames.contains(field.getFieldName())) {
                    throw new GungnirTopologyException(
                        "Field of same field name exists in " + operator.getName());
                  }
                  fieldNames.add(field.getFieldName());
                }
              } else {
                if (fieldNames.contains(field.getFieldName())) {
                  throw new GungnirTopologyException(
                      "Field of same field name exists in " + operator.getName());
                }
                fieldNames.add(field.getFieldName());
              }
            }

            outFieldsMap.put(entry.getKey(), fieldNames);
          }
          outputFields.put(operator.getName(), outFieldsMap);
        } else {
          outputFields.put(operator.getName(), inFieldsMap);
        }
      }
    }

    return outputFields;
  }

  private Map<String, GroupFields> getGroupFields(
      DefaultDirectedGraph<Operator, StreamEdge> graph, List<Operator> operators) {
    Map<String, GroupFields> groupFields = Maps.newLinkedHashMap();

    for (Operator operator : operators) {
      if (graph.inDegreeOf(operator) > 0) {
        StreamEdge edge = graph.incomingEdgesOf(operator).iterator().next();
        if (edge instanceof GroupedStreamEdge) {
          groupFields.put(operator.getName(), ((GroupedStreamEdge) edge).getGroupFields());
        }
      }
    }

    return groupFields;
  }

  private List<Schema> selectSchemas(Schema[] schemas, TupleAccessor[] selector) {
    if (selector != null) {
      List<Schema> select = Lists.newArrayList();
      for (TupleAccessor tuple : selector) {
        for (Schema schema : schemas) {
          if (schema.getSchemaName().equals(tuple.getTupleName())) {
            select.add(schema);
          }
        }
      }
      return select;
    } else {
      return Lists.newArrayList(schemas);
    }
  }

  private List<Operator> phase1(DefaultDirectedGraph<Operator, StreamEdge> graph) {
    int defParallelism = config.getInteger(DEFAULT_PARALLELISM);

    BreadthFirstIterator<Operator, StreamEdge> it =
        new BreadthFirstIterator<Operator, StreamEdge>(graph);
    List<Operator> operators = Lists.newArrayList(it);

    for (Operator operator : operators) {
      if (operator.getParallelism() == 0 && !(operator instanceof PartitionOperator)) {
        operator.setParallelism(defParallelism);
      }

      if (operator instanceof SpoutOperator) {
        List<StreamEdge> edges = Lists.newArrayList(graph.outgoingEdgesOf(operator));
        PartitionOperator partitionOperator = null;

        for (StreamEdge edge : edges) {
          Operator target = graph.getEdgeTarget(edge);

          if (!(target instanceof PartitionOperator) && !(target instanceof MergeOperator)) {
            if (partitionOperator == null) {
              List<Schema> schemas = selectSchemas(((SpoutOperator) operator).getSchemas(),
                  edge.getSelector());
              Grouping grouping = GroupingBuilder.forSchemas(schemas).build();

              partitionOperator = new PartitionOperator(grouping);
              graph.addVertex(partitionOperator);
              graph.addEdge(operator, partitionOperator, edge.clone());
              graph.addEdge(partitionOperator, target, edge.clone());
              graph.removeEdge(edge);
            } else {
              StreamEdge edge2 = graph.getEdge(operator, partitionOperator);
              TupleAccessor[] selector = mergeSelector(edge2.getSelector(), edge.getSelector());
              edge2.setSelector(selector);

              List<Schema> schemas = selectSchemas(((SpoutOperator) operator).getSchemas(),
                  selector);
              Grouping grouping = GroupingBuilder.forSchemas(schemas).build();
              partitionOperator.setGrouping(grouping);

              graph.addEdge(partitionOperator, target, edge.clone());
              graph.removeEdge(edge);
            }
          } else {
            if (partitionOperator != null) {
              StreamEdge edgeCopy = edge.clone();
              graph.removeEdge(edge);
              graph.addEdge(operator, target, edgeCopy);
            }
          }
        }
      } else if (!(operator instanceof PartitionOperator)) {
        List<StreamEdge> edges = Lists.newArrayList(graph.outgoingEdgesOf(operator));

        for (StreamEdge edge : edges) {
          Operator target = graph.getEdgeTarget(edge);
          if (target.getParallelism() == 0) {
            target.setParallelism(defParallelism);
          }

          if (!(edge instanceof GroupedStreamEdge)
              && !(target instanceof PartitionOperator || target instanceof MergeOperator)
              && (operator.getParallelism() != target.getParallelism())
              && (operator.getParallelism() == 1 || target.getParallelism() == 1)) {
            PartitionOperator partitionOperator = null;
            if (target.getParallelism() == 1) {
              partitionOperator = new PartitionOperator(new GlobalGrouping());
            } else if (operator.getParallelism() == 1) {
              partitionOperator = new PartitionOperator(new ShuffleGrouping());
            }

            if (partitionOperator != null) {
              graph.addVertex(partitionOperator);
              graph.addEdge(operator, partitionOperator, edge.clone());
              graph.addEdge(partitionOperator, target, edge.clone());
              graph.removeEdge(edge);
            }
          }
        }
      }
    }

    operators = Lists.newArrayList();
    it = new BreadthFirstIterator<Operator, StreamEdge>(graph);
    int operatorId = 0;
    while (it.hasNext()) {
      Operator operator = it.next();
      operator.setId(operatorId++);
      operators.add(operator);
    }

    return operators;
  }

  private void phase2(DefaultDirectedGraph<Operator, StreamEdge> graph, List<Operator> operators) {
    for (Operator operator : operators) {
      if (operator instanceof PartitionOperator) {
        PartitionOperator partitionOperator = ((PartitionOperator) operator).clone();
        graph.addVertex(partitionOperator);

        List<StreamEdge> edges = Lists.newArrayList(graph.incomingEdgesOf(operator));
        for (StreamEdge edge : edges) {
          graph.addEdge(graph.getEdgeSource(edge), partitionOperator, edge.clone());
          graph.removeEdge(edge);
        }
      }
    }
  }

  private Dispatcher edgeToDispatcher(DefaultDirectedGraph<Operator, StreamEdge> graph,
      Operator operator, StreamEdge outgoingEdge) {
    Dispatcher dispatcher = null;
    if (outgoingEdge instanceof GroupedStreamEdge) {
      dispatcher = new GroupingDispatcher(graph.getEdgeTarget(outgoingEdge),
          ((GroupedStreamEdge) outgoingEdge).getGroupFields());
    } else {
      dispatcher = new SingleDispatcher(graph.getEdgeTarget(outgoingEdge));
    }
    dispatcher.setSource(operator);

    Set<TupleAccessor> tuples = Sets.newLinkedHashSet();
    for (StreamEdge edge : graph.incomingEdgesOf(operator)) {
      if (edge.getSelector() != null) {
        for (TupleAccessor tuple : edge.getSelector()) {
          tuples.add(tuple);
        }
      }
    }

    if (outgoingEdge.getSelector() == null) {
      return dispatcher;
    } else {
      if (tuples.equals(Sets.newHashSet(outgoingEdge.getSelector()))) {
        return dispatcher;
      } else {
        return FilterDispatcher.builder().filter(new TupleNameFilter(outgoingEdge.getSelector()))
            .dispatcher(dispatcher).source(operator).build();
      }
    }
  }

  private Dispatcher edgesToDispatcher(DefaultDirectedGraph<Operator, StreamEdge> graph,
      Operator operator) {
    if (graph.outDegreeOf(operator) == 1) {
      StreamEdge edge = graph.outgoingEdgesOf(operator).iterator().next();
      return edgeToDispatcher(graph, operator, edge);
    } else {
      MultiDispatcher multiDispatcher = new MultiDispatcher();
      Set<StreamEdge> edges = graph.outgoingEdgesOf(operator);
      for (StreamEdge edge : edges) {
        multiDispatcher.addDispatcher(edgeToDispatcher(graph, operator, edge));
      }
      multiDispatcher.setSource(operator);
      return multiDispatcher;
    }
  }

  private void collectMetrics(Operator operator, Map<String, Metrics> metricsMap) {
    Map<String, Metrics> metrics = operator.getMetrics();
    if (metrics != null) {
      for (Map.Entry<String, Metrics> entry : metrics.entrySet()) {
        metricsMap.put(entry.getKey() + '(' + operator.getName() + ')', entry.getValue());
      }
    }
  }

  private String phase3(DefaultDirectedGraph<Operator, StreamEdge> graph,
      List<Operator> operators, Map<String, Map<String, List<String>>> outputFields,
      Map<String, GroupFields> groupFields, boolean explain)
      throws GungnirTopologyException {
    context = new GungnirContext();
    context.setTopologyId(id);
    context.setTopologyName(name);
    context.setAccountId(owner.getId());
    context.setOutputFields(outputFields);
    context.setGroupFields(groupFields);

    ExecBolt bolt = null;
    bolts = Lists.newArrayList();
    boltsIndex = Maps.newHashMap();

    if (!explain) {
      builder = new TopologyBuilder();
      boltDeclarers = Lists.newArrayList();
    }

    StringBuilder sb = new StringBuilder();
    sb.append("Components:\n");

    for (Operator operator : operators) {
      if (graph.inDegreeOf(operator) == 0) {
        Integer boltIndex = null;
        if (operator instanceof SpoutOperator) {
          spout = new ExecSpout(context);

          sb.append(' ');
          sb.append(spout.getName());
          sb.append(" {");
        } else if (operator instanceof PartitionOperator) {
          PartitionOperator incomingOperator = (PartitionOperator) operator;
          boltIndex = boltsIndex.get(incomingOperator.getGrouping());
          if (boltIndex == null) {
            boltIndex = bolts.size();
            bolt = new ExecBolt(context);
            bolt.setId(boltIndex + 1);

            bolts.add(bolt);
          } else {
            bolt = bolts.get(boltIndex);
          }

          sb.append(' ');
          sb.append(bolt.getName());
          sb.append(" {");
        }

        List<PartitionOperator> outgoingOperators = Lists.newArrayList();
        BreadthFirstIterator<Operator, StreamEdge> it =
            new BreadthFirstIterator<Operator, StreamEdge>(graph, operator);
        int parallelism = 0;
        Map<String, Metrics> metricsMap = Maps.newHashMap();

        while (it.hasNext()) {
          Operator operator2 = it.next();
          if (graph.outDegreeOf(operator2) == 0) {
            if (operator2 instanceof PartitionOperator) {
              outgoingOperators.add((PartitionOperator) operator2);
            }
          } else {
            Dispatcher dispatcher = edgesToDispatcher(graph, operator2);
            operator2.setDispatcher(dispatcher);
            operator2.registerMetrics(METRICS_DISPATCH_COUNT, new MultiCountMeter());

            sb.append("\n  ");
            sb.append(operator2.getName());
            sb.append(' ');
            sb.append(dispatcher);
          }

          if (operator2.getParallelism() > parallelism) {
            parallelism = operator2.getParallelism();
          }

          collectMetrics(operator2, metricsMap);
        }

        sb.append("\n } parallelism=");
        sb.append(parallelism);
        sb.append('\n');

        if (operator instanceof SpoutOperator) {
          spout.setIncomingOperator((SpoutOperator) operator);
          spout.addOutgoingOperators(outgoingOperators);
          spout.registerMetrics(metricsMap);

          if (!explain) {
            builder.setSpout(spout.getName(), spout, parallelism);
          }
        } else if (operator instanceof PartitionOperator) {
          PartitionOperator incomingOperator = (PartitionOperator) operator;
          bolt.addIncomingOperator(incomingOperator);
          bolt.addOutgoingOperators(outgoingOperators);
          bolt.registerMetrics(metricsMap);

          if (!boltsIndex.containsKey(incomingOperator.getGrouping())) {
            if (!explain) {
              boltDeclarers.add(builder.setBolt(bolt.getName(), bolt, parallelism));
            }
            boltsIndex.put(incomingOperator.getGrouping(), boltIndex);
          }
        }
      }
    }

    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

  private void phase4() {
    for (PartitionOperator partitionOperator : spout.getOutgoingOperators()) {
      Integer boltIndex = boltsIndex.get(partitionOperator.getGrouping());
      if (boltIndex != null) {
        partitionOperator.getGrouping().setContext(context);
        partitionOperator.getGrouping().setPartitionName(partitionOperator.getName());

        boltDeclarers.get(boltIndex).customGrouping(spout.getName(), partitionOperator.getName(),
            partitionOperator.getGrouping());
      }
    }

    for (ExecBolt bolt : bolts) {
      for (PartitionOperator partitionOperator : bolt.getOutgoingOperators()) {
        Integer index = boltsIndex.get(partitionOperator.getGrouping());
        if (index != null) {
          partitionOperator.getGrouping().setContext(context);
          partitionOperator.getGrouping().setPartitionName(partitionOperator.getName());

          boltDeclarers.get(index).customGrouping(bolt.getName(),
              partitionOperator.getName(), partitionOperator.getGrouping());
        }
      }
    }
  }

  private String explainGraph(DefaultDirectedGraph<Operator, StreamEdge> graph,
      List<Operator> operators) {
    StringBuilder sb = new StringBuilder();
    sb.append("Explain:\n");

    for (Operator operator : operators) {
      sb.append(' ');
      sb.append(operator);
      if (!(operator instanceof PartitionOperator)) {
        sb.append(" parallelism=");
        sb.append(operator.getParallelism());
      }
      sb.append('\n');
      for (StreamEdge edge : graph.outgoingEdgesOf(operator)) {
        sb.append("  -");
        sb.append(edge);
        sb.append("-> ");
        sb.append(graph.getEdgeTarget(edge).getName());
        sb.append('\n');
      }
    }

    return sb.toString();
  }

  private String explainStreamEdges(DefaultDirectedGraph<Operator, StreamEdge> graph,
      List<Operator> operators) {
    StringBuilder sb = new StringBuilder();
    sb.append("Stream edges:\n");

    for (Operator operator : operators) {
      sb.append(' ');
      sb.append(operator.getName());
      sb.append("\n  incoming: ");
      if (graph.inDegreeOf(operator) == 0) {
        sb.append('-');
      } else {
        int i = 0;
        for (StreamEdge edge : graph.incomingEdgesOf(operator)) {
          if (i > 0) {
            sb.append(", ");
          }
          sb.append(graph.getEdgeSource(edge).getName());
          i++;
        }
      }
      sb.append("\n  outgoing: ");
      if (graph.outDegreeOf(operator) == 0) {
        sb.append('-');
      } else {
        int i = 0;
        for (StreamEdge edge : graph.outgoingEdgesOf(operator)) {
          if (i > 0) {
            sb.append(", ");
          }
          sb.append(graph.getEdgeTarget(edge).getName());
          i++;
        }
      }
      sb.append('\n');
    }

    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

  private String explainOutputFields(Map<String, Map<String, List<String>>> outputFields)
      throws GungnirTopologyException {
    StringBuilder sb = new StringBuilder();
    sb.append("Output fields:\n");

    for (Map.Entry<String, Map<String, List<String>>> entry : outputFields.entrySet()) {
      sb.append(' ');
      sb.append(entry.getKey());
      sb.append(' ');
      sb.append(entry.getValue());
      sb.append('\n');
    }

    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

  private String explainGroupFields(Map<String, GroupFields> groupFields) {
    StringBuilder sb = new StringBuilder();
    sb.append("Group fields:\n");

    for (Map.Entry<String, GroupFields> entry : groupFields.entrySet()) {
      sb.append(' ');
      sb.append(entry.getKey());
      sb.append(' ');
      sb.append(entry.getValue());
      sb.append('\n');
    }

    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

  private String explainStormTopology() {
    StringBuilder sb = new StringBuilder();
    sb.append("Topology:\n ");

    sb.append(spout.getName());
    sb.append('\n');
    for (PartitionOperator partitionOperator : spout.getOutgoingOperators()) {
      Integer index = boltsIndex.get(partitionOperator.getGrouping());
      if (index != null) {
        sb.append("  -");
        sb.append(partitionOperator.getName());
        sb.append("-> ");
        sb.append(bolts.get(index).getName());
        sb.append('\n');
      }
    }

    for (ExecBolt bolt : bolts) {
      sb.append(' ');
      sb.append(bolt.getName());
      sb.append('\n');
      for (PartitionOperator partitionOperator : bolt.getOutgoingOperators()) {
        Integer index = boltsIndex.get(partitionOperator.getGrouping());
        if (index != null) {
          sb.append("  -");
          sb.append(partitionOperator.getName());
          sb.append("-> ");
          sb.append(bolts.get(index).getName());
          sb.append('\n');
        }
      }
    }

    sb.deleteCharAt(sb.length() - 1);
    return sb.toString();
  }

  @SuppressWarnings("unchecked")
  public String explain(boolean extended) throws GungnirTopologyException {
    if (graph.vertexSet().size() == 0) {
      LOG.info("Topology isn't registered");
      throw new GungnirTopologyException("Topology isn't registered");
    }

    DefaultDirectedGraph<Operator, StreamEdge> graphCopy =
        (DefaultDirectedGraph<Operator, StreamEdge>) graph.clone();

    List<Operator> operators = Lists.newArrayList();
    if (LOG.isDebugEnabled()) {
      BreadthFirstIterator<Operator, StreamEdge> it =
          new BreadthFirstIterator<Operator, StreamEdge>(graph);
      int operatorId = 0;
      while (it.hasNext()) {
        Operator operator = it.next();
        operator.setId(operatorId++);
        operators.add(operator);
      }
      LOG.debug(explainGraph(graphCopy, operators));
    }

    operators = phase1(graphCopy);

    StringBuilder sb = new StringBuilder();

    sb.append(explainGraph(graphCopy, operators));

    if (extended) {
      sb.append(explainStreamEdges(graphCopy, operators));

      Map<String, Map<String, List<String>>> outputFields = getOutputFields(graphCopy, operators);
      sb.append('\n');
      sb.append(explainOutputFields(outputFields));

      Map<String, GroupFields> groupFields = getGroupFields(graphCopy, operators);
      sb.append('\n');
      sb.append(explainGroupFields(groupFields));

      phase2(graphCopy, operators);

      sb.append('\n');
      sb.append(phase3(graphCopy, operators, outputFields, groupFields, true));

      sb.append('\n');
      sb.append(explainStormTopology());
    } else {
      sb.deleteCharAt(sb.length() - 1);
    }

    String explain = sb.toString();
    LOG.debug(explain);
    return explain;
  }

  @SuppressWarnings("unchecked")
  public StormTopology build() throws GungnirTopologyException {
    if (graph.vertexSet().size() == 0) {
      LOG.info("Operator isn't registered");
      throw new GungnirTopologyException("Operator isn't registered");
    }

    LOG.info("Topology build start");

    DefaultDirectedGraph<Operator, StreamEdge> graphCopy =
        (DefaultDirectedGraph<Operator, StreamEdge>) graph.clone();

    List<Operator> operators = phase1(graphCopy);

    LOG.info(explainGraph(graphCopy, operators));

    Map<String, Map<String, List<String>>> outputFields = getOutputFields(graphCopy, operators);
    LOG.info(explainOutputFields(outputFields));

    Map<String, GroupFields> groupFields = getGroupFields(graphCopy, operators);
    LOG.info(explainGroupFields(groupFields));

    phase2(graphCopy, operators);

    LOG.info(phase3(graphCopy, operators, outputFields, groupFields, false));

    LOG.info(explainStormTopology());

    phase4();

    return builder.createTopology();
  }

  @Override
  public GungnirTopology clone() {
    return new GungnirTopology(this);
  }
}
