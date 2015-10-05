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

package org.gennai.gungnir.ql.analysis;

import static org.gennai.gungnir.ql.analysis.GungnirLexer.*;

import java.util.Map;
import java.util.Set;

import org.gennai.gungnir.GungnirTopology;
import org.gennai.gungnir.GungnirTopologyException;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.ql.SchemaRegistry;
import org.gennai.gungnir.ql.analysis.analyzer.AggregeteExprsAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.AlterUserStmtAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.Analyzer;
import org.gennai.gungnir.ql.analysis.analyzer.ArgumentsAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.ArrayAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.BeginGroupClauseAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.CastAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.CommentAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.ComplexConditionAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.ConditionAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.ConstantAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.CountIntervalAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.CreateFunctionStmtAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.CreateTupleStmtAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.CreateUserStmtAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.CreateViewStmtAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.CronIntervalAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.DescTopologyStmtAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.DistinctAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.EachClauseAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.EmitClauseAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.EvalAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.ExplainStmtAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.FieldAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.FieldTypeAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.FieldsAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.FilterClauseAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.FilterGroupClauseAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.FromClauseAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.FunctionAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.IdentifierAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.JoinClauseAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.LimitClauseAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.MapAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.ParallelismAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.PeriodAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.ProcessorAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.SimpleConditionAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.SlideClauseAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.SnapshotClauseAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.StatsTopologyStmtAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.SubmitTopologyStmtAnalyzer;
import org.gennai.gungnir.ql.analysis.analyzer.TimeIntervalAnalyzer;
import org.gennai.gungnir.ql.session.StatementEntity;
import org.gennai.gungnir.ql.stream.GroupedStream;
import org.gennai.gungnir.ql.stream.Stream;
import org.gennai.gungnir.ql.task.ClearTask;
import org.gennai.gungnir.ql.task.DescClusterTask;
import org.gennai.gungnir.ql.task.DescFunctionTask;
import org.gennai.gungnir.ql.task.DescTupleTask;
import org.gennai.gungnir.ql.task.DescUserTask;
import org.gennai.gungnir.ql.task.DescViewTask;
import org.gennai.gungnir.ql.task.DropFunctionTask;
import org.gennai.gungnir.ql.task.DropSchemaTask;
import org.gennai.gungnir.ql.task.DropTopologyTask;
import org.gennai.gungnir.ql.task.DropUserTask;
import org.gennai.gungnir.ql.task.ShowFunctionsTask;
import org.gennai.gungnir.ql.task.ShowTopologiesTask;
import org.gennai.gungnir.ql.task.ShowTuplesTask;
import org.gennai.gungnir.ql.task.ShowUsersTask;
import org.gennai.gungnir.ql.task.ShowViewsTask;
import org.gennai.gungnir.ql.task.StartTopologyTask;
import org.gennai.gungnir.ql.task.StopTopologyTask;
import org.gennai.gungnir.ql.task.Task;

import com.google.common.collect.Maps;

public class SemanticAnalyzer {

  private StatementEntity statement;
  private Map<Integer, Analyzer<?>> analyzersMap;
  private SchemaRegistry schemaRegistry;
  private FileRegistry fileRegistry;
  private ProcessorRegistry processorRegistry;
  private FunctionRegistry functionRegistry;
  private Task task;

  public SemanticAnalyzer(StatementEntity statement) throws SemanticAnalyzeException {
    this.statement = statement;

    analyzersMap = Maps.newHashMap();

    analyzersMap.put(TOK_CAST, new CastAnalyzer());
    analyzersMap.put(TOK_DISTINCT, new DistinctAnalyzer());
    analyzersMap.put(TOK_AGGREGATE_EXPRS, new AggregeteExprsAnalyzer());

    analyzersMap.put(TOK_EXPLAIN, new ExplainStmtAnalyzer());
    analyzersMap.put(TOK_CREATE_TUPLE, new CreateTupleStmtAnalyzer());
    analyzersMap.put(TOK_CREATE_VIEW, new CreateViewStmtAnalyzer());
    analyzersMap.put(TOK_CREATE_FUNCTION, new CreateFunctionStmtAnalyzer());
    analyzersMap.put(TOK_CREATE_USER, new CreateUserStmtAnalyzer());
    analyzersMap.put(TOK_ALTER_USER, new AlterUserStmtAnalyzer());
    analyzersMap.put(TOK_SUBMIT_TOPOLOGY, new SubmitTopologyStmtAnalyzer());
    analyzersMap.put(TOK_DESC_TOPOLOGY, new DescTopologyStmtAnalyzer());
    analyzersMap.put(TOK_STATS_TOPOLOGY, new StatsTopologyStmtAnalyzer());

    FieldTypeAnalyzer fieldTypeAnalyzer = new FieldTypeAnalyzer();
    analyzersMap.put(STRING, fieldTypeAnalyzer);
    analyzersMap.put(TINYINT, fieldTypeAnalyzer);
    analyzersMap.put(SMALLINT, fieldTypeAnalyzer);
    analyzersMap.put(INT, fieldTypeAnalyzer);
    analyzersMap.put(BIGINT, fieldTypeAnalyzer);
    analyzersMap.put(FLOAT, fieldTypeAnalyzer);
    analyzersMap.put(DOUBLE, fieldTypeAnalyzer);
    analyzersMap.put(BOOLEAN, fieldTypeAnalyzer);
    analyzersMap.put(TIMESTAMP, fieldTypeAnalyzer);
    analyzersMap.put(LIST, fieldTypeAnalyzer);
    analyzersMap.put(MAP, fieldTypeAnalyzer);
    analyzersMap.put(STRUCT, fieldTypeAnalyzer);

    analyzersMap.put(TOK_FIELD, new FieldAnalyzer());
    analyzersMap.put(TOK_FIELDS, new FieldsAnalyzer());
    analyzersMap.put(TOK_ARRAY, new ArrayAnalyzer());
    analyzersMap.put(TOK_MAP, new MapAnalyzer());

    analyzersMap.put(TOK_CONDITION, new ConditionAnalyzer());

    SimpleConditionAnalyzer simpleConditionAnalyzer = new SimpleConditionAnalyzer();
    analyzersMap.put(TOK_OP_EQ, simpleConditionAnalyzer);
    analyzersMap.put(TOK_OP_NE, simpleConditionAnalyzer);
    analyzersMap.put(TOK_OP_LE, simpleConditionAnalyzer);
    analyzersMap.put(TOK_OP_LT, simpleConditionAnalyzer);
    analyzersMap.put(TOK_OP_GE, simpleConditionAnalyzer);
    analyzersMap.put(TOK_OP_GT, simpleConditionAnalyzer);
    analyzersMap.put(TOK_OP_LIKE, simpleConditionAnalyzer);
    analyzersMap.put(TOK_OP_REGEXP, simpleConditionAnalyzer);
    analyzersMap.put(TOK_OP_IN, simpleConditionAnalyzer);
    analyzersMap.put(TOK_OP_ALL, simpleConditionAnalyzer);
    analyzersMap.put(TOK_OP_BETWEEN, simpleConditionAnalyzer);
    analyzersMap.put(TOK_OP_IS_NULL, simpleConditionAnalyzer);
    analyzersMap.put(TOK_OP_IS_NOT_NULL, simpleConditionAnalyzer);

    ComplexConditionAnalyzer complexConditionAnalyzer = new ComplexConditionAnalyzer();
    analyzersMap.put(TOK_OP_OR, complexConditionAnalyzer);
    analyzersMap.put(TOK_OP_AND, complexConditionAnalyzer);
    analyzersMap.put(TOK_OP_NOT, complexConditionAnalyzer);

    PeriodAnalyzer periodAnalyzer = new PeriodAnalyzer();
    analyzersMap.put(SecondsLiteral, periodAnalyzer);
    analyzersMap.put(MinutesLiteral, periodAnalyzer);
    analyzersMap.put(HoursLiteral, periodAnalyzer);
    analyzersMap.put(DaysLiteral, periodAnalyzer);

    analyzersMap.put(TOK_PROCESSOR, new ProcessorAnalyzer());
    analyzersMap.put(TOK_FUNCTION, new FunctionAnalyzer());
    analyzersMap.put(TOK_ARGUMENTS, new ArgumentsAnalyzer());

    EvalAnalyzer evalAnalyzer = new EvalAnalyzer();
    analyzersMap.put(PLUS, evalAnalyzer);
    analyzersMap.put(MINUS, evalAnalyzer);
    analyzersMap.put(ASTERISK, evalAnalyzer);
    analyzersMap.put(SLASH, evalAnalyzer);
    analyzersMap.put(PERCENT, evalAnalyzer);
    analyzersMap.put(DIV, evalAnalyzer);
    analyzersMap.put(MOD, evalAnalyzer);

    analyzersMap.put(TOK_COUNT_INTERVAL, new CountIntervalAnalyzer());
    analyzersMap.put(TOK_TIME_INTERVAL, new TimeIntervalAnalyzer());
    analyzersMap.put(TOK_CRON_INTERVAL, new CronIntervalAnalyzer());

    analyzersMap.put(TOK_PARALLELISM, new ParallelismAnalyzer());
    analyzersMap.put(TOK_COMMENT, new CommentAnalyzer());

    ConstantAnalyzer constantAnalyzer = new ConstantAnalyzer();
    analyzersMap.put(StringLiteral, constantAnalyzer);
    analyzersMap.put(TinyintLiteral, constantAnalyzer);
    analyzersMap.put(SmallintLiteral, constantAnalyzer);
    analyzersMap.put(IntLiteral, constantAnalyzer);
    analyzersMap.put(BigintLiteral, constantAnalyzer);
    analyzersMap.put(FloatLiteral, constantAnalyzer);
    analyzersMap.put(DoubleLiteral, constantAnalyzer);
    analyzersMap.put(TRUE, constantAnalyzer);
    analyzersMap.put(FALSE, constantAnalyzer);

    IdentifierAnalyzer identifierAnalyzer = new IdentifierAnalyzer();
    analyzersMap.put(Identifier, identifierAnalyzer);

    schemaRegistry = new SchemaRegistry();

    fileRegistry = new FileRegistry(statement);

    functionRegistry = new FunctionRegistry(fileRegistry);

    try {
      processorRegistry = new ProcessorRegistry();
    } catch (Exception e) {
      throw new SemanticAnalyzeException(e);
    }
  }

  public void setStatement(StatementEntity statement) {
    this.statement = statement;
  }

  public SchemaRegistry getSchemaRegistry() {
    return schemaRegistry;
  }

  public FileRegistry getFileRegistry() {
    return fileRegistry;
  }

  public FunctionRegistry getFunctionRegistry() {
    return functionRegistry;
  }

  public ProcessorRegistry getProcessorRegistry() {
    return processorRegistry;
  }

  public UserEntity getOwner() {
    return statement.getOwner();
  }

  public GungnirTopology getTopology() {
    return statement.getTopology();
  }

  public Stream getStream(String streamName) {
    if (statement.getStreamsMap() == null) {
      statement.setStreamsMap(Maps.<String, Stream>newHashMap());
    }
    return statement.getStreamsMap().get(streamName);
  }

  public Task getTask() {
    return task;
  }

  public Set<String> getStreamTuples() {
    return statement.getStreamTuples();
  }

  public Map<String, String> getAliasNamesMap() {
    return statement.getAliasNamesMap();
  }

  @SuppressWarnings("unchecked")
  public <T> T analyzeByAnalyzer(ASTNode node) throws SemanticAnalyzeException {
    if (node == null) {
      return null;
    }
    return (T) analyzersMap.get(node.getType()).analyze(node, this);
  }

  private void intoClauseAnalyze(ASTNode node, Stream stream)
      throws SemanticAnalyzeException {
    String streamName = analyzeByAnalyzer(node.getChild(0));
    statement.getStreamsMap().put(streamName, stream);
  }

  private Stream queryAnalyze(ASTNode node, Stream stream)
      throws SemanticAnalyzeException, GungnirTopologyException {
    switch (node.getType()) {
      case TOK_FROM:
        return new FromClauseAnalyzer(this).analyze(node);
      case TOK_EMIT:
        return new EmitClauseAnalyzer(this).analyze(node, stream);
      case TOK_INTO:
        intoClauseAnalyze(node, stream);
        return stream;
      case TOK_JOIN:
        return new JoinClauseAnalyzer(this).analyze(node, stream);
      case TOK_EACH:
        return new EachClauseAnalyzer(this).analyze(node, stream);
      case TOK_FILTER:
        return new FilterClauseAnalyzer(this).analyze(node, stream);
      case TOK_FILTER_GROUP:
        return new FilterGroupClauseAnalyzer(this).analyze(node, stream);
      case TOK_SLIDE:
        return new SlideClauseAnalyzer(this).analyze(node, stream);
      case TOK_SNAPSHOT:
        return new SnapshotClauseAnalyzer(this).analyze(node, stream);
      case TOK_LIMIT:
        return new LimitClauseAnalyzer(this).analyze(node, stream);
      case TOK_BEGIN_GROUP:
        return new BeginGroupClauseAnalyzer(this).analyze(node, stream);
      case TOK_END_GROUP:
        if (stream instanceof GroupedStream<?>) {
          return ((GroupedStream<?>) stream).endGroup();
        }
        return stream;
      case TOK_TO_STREAM:
        if (stream instanceof GroupedStream<?>) {
          return ((GroupedStream<?>) stream).toStream();
        }
        return stream;
      default:
        return stream;
    }
  }

  public void analyze(ASTNode node) throws SemanticAnalyzeException {
    Stream stream = null;

    switch (node.getType()) {
      case TOK_QUERY:
        try {
          for (int i = 0; i < node.getChildCount(); i++) {
            stream = queryAnalyze(node.getChild(i), stream);
          }
        } catch (GungnirTopologyException e) {
          throw new SemanticAnalyzeException(e.getMessage(), e);
        }
        task = null;
        break;
      case TOK_CLEAR:
        task = new ClearTask(this);
        break;
      case TOK_DESC_TUPLE:
        String tupleName = analyzeByAnalyzer(node.getChild(0));
        task = new DescTupleTask(tupleName, statement.getOwner());
        break;
      case TOK_SHOW_TUPLES:
        task = new ShowTuplesTask(statement.getOwner());
        break;
      case TOK_DROP_TUPLE:
        tupleName = analyzeByAnalyzer(node.getChild(0));
        task = new DropSchemaTask(tupleName, statement.getOwner());
        break;
      case TOK_DESC_VIEW:
        String viewName = analyzeByAnalyzer(node.getChild(0));
        task = new DescViewTask(viewName, statement.getOwner());
        break;
      case TOK_SHOW_VIEWS:
        task = new ShowViewsTask(statement.getOwner());
        break;
      case TOK_DROP_VIEW:
        viewName = analyzeByAnalyzer(node.getChild(0));
        task = new DropSchemaTask(viewName, statement.getOwner());
        break;
      case TOK_DESC_FUNCTION:
        String functionName = analyzeByAnalyzer(node.getChild(0));
        task = new DescFunctionTask(functionName, statement.getOwner());
        break;
      case TOK_SHOW_FUNCTIONS:
        task = new ShowFunctionsTask(statement.getOwner());
        break;
      case TOK_DROP_FUNCTION:
        functionName = analyzeByAnalyzer(node.getChild(0));
        task = new DropFunctionTask(functionName, statement.getOwner());
        break;
      case TOK_DESC_USER:
        task = new DescUserTask(statement.getOwner());
        break;
      case TOK_SHOW_USERS:
        task = new ShowUsersTask(statement.getOwner());
        break;
      case TOK_DROP_USER:
        String userName = analyzeByAnalyzer(node.getChild(0));
        task = new DropUserTask(userName, statement.getOwner());
        break;
      case TOK_SHOW_TOPOLOGIES:
        task = new ShowTopologiesTask(statement.getOwner());
        break;
      case TOK_STOP_TOPOLOGY:
        String topologyName = analyzeByAnalyzer(node.getChild(0));
        task = new StopTopologyTask(topologyName, statement.getOwner());
        break;
      case TOK_START_TOPOLOGY:
        topologyName = analyzeByAnalyzer(node.getChild(0));
        task = new StartTopologyTask(topologyName, statement.getOwner(), fileRegistry);
        break;
      case TOK_DROP_TOPOLOGY:
        topologyName = analyzeByAnalyzer(node.getChild(0));
        task = new DropTopologyTask(topologyName, statement.getOwner(), fileRegistry);
        break;
      case TOK_DESC_CLUSTER:
        task = new DescClusterTask(statement.getOwner());
        break;
      default:
        task = analyzeByAnalyzer(node);
        break;
    }
  }

  public void clear() {
    statement.clear();
    statement.setTopology(new GungnirTopology(statement.getConfig(), statement.getOwner()));
  }
}
