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

package org.gennai.gungnir.ql.analysis.analyzer;

import static org.gennai.gungnir.ql.analysis.GungnirLexer.*;

import java.util.List;
import java.util.Set;

import org.gennai.gungnir.GungnirTopologyException;
import org.gennai.gungnir.Period;
import org.gennai.gungnir.ql.analysis.ASTNode;
import org.gennai.gungnir.ql.analysis.SemanticAnalyzeException;
import org.gennai.gungnir.ql.stream.SingleStream;
import org.gennai.gungnir.ql.stream.Stream;
import org.gennai.gungnir.ql.stream.TupleJoinStreamBuilder.JoinToTupleDeclarer;
import org.gennai.gungnir.topology.grouping.GroupingBuilder;
import org.gennai.gungnir.topology.processor.InMemoryTtlCacheProcessor;
import org.gennai.gungnir.topology.processor.Processor;
import org.gennai.gungnir.topology.processor.SpoutProcessor;
import org.gennai.gungnir.topology.processor.TtlCacheProcessor;
import org.gennai.gungnir.tuple.Condition;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.TupleAccessor;
import org.gennai.gungnir.tuple.schema.Schema;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class FromSpoutClauseAnalyzer {

  private FromClauseAnalyzer fromClauseAnalyzer;

  public FromSpoutClauseAnalyzer(FromClauseAnalyzer fromClauseAnalyzer) {
    this.fromClauseAnalyzer = fromClauseAnalyzer;
  }

  private static class JoinSchemaDeclare {

    private Schema joinSchema;
    private Condition joinCondition;
    private Integer parallelism;
  }

  private JoinSchemaDeclare joinSchemaExprAnalyze(ASTNode node, Set<Schema> schemasSet)
      throws SemanticAnalyzeException {
    JoinSchemaDeclare declare = new JoinSchemaDeclare();

    String schemaName =
        fromClauseAnalyzer.getSemanticAnalyzer().analyzeByAnalyzer(node.getChild(0));
    Schema schema = fromClauseAnalyzer.getSemanticAnalyzer().getSchemaRegistry().get(schemaName);
    if (schema == null) {
      throw new SemanticAnalyzeException(schemaName + " isn't registered");
    }
    declare.joinSchema = schema;
    schemasSet.add(schema);

    Condition joinCondition =
        fromClauseAnalyzer.getSemanticAnalyzer().analyzeByAnalyzer(node.getChild(1));
    declare.joinCondition = joinCondition;

    if (node.getChildCount() > 2) {
      declare.parallelism =
          fromClauseAnalyzer.getSemanticAnalyzer().analyzeByAnalyzer(node.getChild(2));
    }

    return declare;
  }

  private List<JoinSchemaDeclare> joinSchemaExprsAnalyze(ASTNode node, Set<Schema> schemasSet)
      throws SemanticAnalyzeException {
    List<JoinSchemaDeclare> declares = Lists.newArrayListWithCapacity(node.getChildCount());
    for (int i = 0; i < node.getChildCount(); i++) {
      declares.add(joinSchemaExprAnalyze(node.getChild(i), schemasSet));
    }
    return declares;
  }

  private static class JoinSchemasDeclare {

    private Schema fromSchema;
    private List<JoinSchemaDeclare> schemaDeclares;
    private FieldAccessor[] toFields;
    private TupleAccessor toTuple;
    private Period expire;
    private TtlCacheProcessor processor;
  }

  public Stream analyze(ASTNode node) throws SemanticAnalyzeException, GungnirTopologyException {
    Set<Schema> schemasSet = Sets.newLinkedHashSet();
    List<Schema> singleSchemas = Lists.newArrayList();
    List<JoinSchemasDeclare> schemasDeclares = null;

    for (int i = 0; i < node.getChild(0).getChildCount(); i++) {
      ASTNode child = node.getChild(0).getChild(i);

      String schemaName =
          fromClauseAnalyzer.getSemanticAnalyzer().analyzeByAnalyzer(child.getChild(0));
      Schema schema = fromClauseAnalyzer.getSemanticAnalyzer().getSchemaRegistry().get(schemaName);
      if (schema == null) {
        throw new SemanticAnalyzeException(schemaName + " isn't registered");
      }
      schemasSet.add(schema);

      if (child.getChild(1) != null && child.getChild(1).getType() == TOK_JOIN_SCHEMA_EXPRS) {
        List<JoinSchemaDeclare> declares = joinSchemaExprsAnalyze(child.getChild(1), schemasSet);

        JoinSchemasDeclare schemasDeclare = new JoinSchemasDeclare();
        schemasDeclare.fromSchema = schema;
        schemasDeclare.schemaDeclares = declares;
        schemasDeclare.toFields = fromClauseAnalyzer.tupleJoinToExprAnalyze(child.getChild(2));
        schemasDeclare.expire = fromClauseAnalyzer.getSemanticAnalyzer()
            .analyzeByAnalyzer(child.getChild(3));

        String toTupleName;
        if (child.getChild(4).getType() == TOK_PROCESSOR) {
          Processor p = fromClauseAnalyzer.getSemanticAnalyzer()
              .analyzeByAnalyzer(child.getChild(4));
          if (p instanceof TtlCacheProcessor) {
            schemasDeclare.processor = (TtlCacheProcessor) p;
          } else {
            throw new SemanticAnalyzeException("Processor isn't cache processor '" + p + "'");
          }

          toTupleName = fromClauseAnalyzer.getSemanticAnalyzer()
              .analyzeByAnalyzer(child.getChild(5));
        } else {
          schemasDeclare.processor = new InMemoryTtlCacheProcessor();

          toTupleName = fromClauseAnalyzer.getSemanticAnalyzer()
              .analyzeByAnalyzer(child.getChild(4));
        }
        schemasDeclare.toTuple = new TupleAccessor(toTupleName);
        fromClauseAnalyzer.getSemanticAnalyzer().getStreamTuples().add(toTupleName);

        if (schemasDeclares == null) {
          schemasDeclares = Lists.newArrayList();
        }
        schemasDeclares.add(schemasDeclare);
      } else {
        singleSchemas.add(schema);
        String aliasName =
            fromClauseAnalyzer.getSemanticAnalyzer().analyzeByAnalyzer(child.getChild(1));
        fromClauseAnalyzer.getSemanticAnalyzer().getSchemaRegistry().register(aliasName,
            schema.getSchemaName());
        fromClauseAnalyzer.getSemanticAnalyzer().getStreamTuples().add(aliasName);
        if (aliasName != null) {
          fromClauseAnalyzer.getSemanticAnalyzer().getAliasNamesMap()
              .put(aliasName, schema.getSchemaName());
        }
      }
    }

    SpoutProcessor processor = null;
    Processor p = fromClauseAnalyzer.getSemanticAnalyzer().analyzeByAnalyzer(node.getChild(1));
    if (p instanceof SpoutProcessor) {
      processor = (SpoutProcessor) p;
    } else {
      throw new SemanticAnalyzeException("Processor isn't spout processor '" + p + "'");
    }

    Integer parallelism =
        fromClauseAnalyzer.getSemanticAnalyzer().analyzeByAnalyzer(node.getChild(2));

    Schema[] schemas = schemasSet.toArray(new Schema[0]);
    SingleStream stream =
        fromClauseAnalyzer.getSemanticAnalyzer().getTopology().from(processor, schemas)
            .parallelism(parallelism);

    if (schemasDeclares == null) {
      return stream;
    } else {
      List<SingleStream> streams = Lists.newArrayListWithCapacity(schemasDeclares.size() + 1);

      List<TupleAccessor> tuples = Lists.newArrayList();
      for (Schema schema : singleSchemas) {
        tuples.add(new TupleAccessor(schema.getSchemaName()));
      }
      if (tuples.size() > 0) {
        List<Schema> selectSchemas = Lists.newArrayList();
        for (int i = 0; i < schemas.length; i++) {
          for (int j = 0; j < tuples.size(); j++) {
            if (schemas[i].getSchemaName().equals(tuples.get(j).getTupleName())) {
              selectSchemas.add(schemas[i]);
            }
          }
        }

        streams.add(stream.select(tuples.toArray(new TupleAccessor[0]))
            .partition(GroupingBuilder.forSchemas(selectSchemas).build()));
      }

      for (JoinSchemasDeclare schemasDeclare : schemasDeclares) {
        SingleStream fromStream = stream.select(new TupleAccessor(schemasDeclare.fromSchema
            .getSchemaName()));
        SingleStream joinStream = stream.select(new TupleAccessor(
            schemasDeclare.schemaDeclares.get(0).joinSchema.getSchemaName()));
        JoinToTupleDeclarer<SingleStream> joinToTupleDeclarer = fromStream.join(
            joinStream, schemasDeclare.schemaDeclares.get(0).joinCondition,
            schemasDeclare.schemaDeclares.get(0).parallelism);
        for (int i = 1; i < schemasDeclare.schemaDeclares.size(); i++) {
          joinStream = stream.select(new TupleAccessor(
              schemasDeclare.schemaDeclares.get(i).joinSchema.getSchemaName()));
          joinToTupleDeclarer =
              joinToTupleDeclarer.join(joinStream,
                  schemasDeclare.schemaDeclares.get(i).joinCondition,
                  schemasDeclare.schemaDeclares.get(i).parallelism);
        }

        SingleStream joinedStream =
            joinToTupleDeclarer.to(schemasDeclare.toTuple, schemasDeclare.toFields)
                .expire(schemasDeclare.expire).using(schemasDeclare.processor).build();
        streams.add(joinedStream);
      }

      return fromClauseAnalyzer.getSemanticAnalyzer().getTopology()
          .from(streams.toArray(new SingleStream[0]));
    }
  }
}
