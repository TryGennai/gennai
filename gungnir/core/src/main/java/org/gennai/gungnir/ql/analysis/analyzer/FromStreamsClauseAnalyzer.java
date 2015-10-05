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

import org.gennai.gungnir.GungnirTopologyException;
import org.gennai.gungnir.Period;
import org.gennai.gungnir.ql.analysis.ASTNode;
import org.gennai.gungnir.ql.analysis.SemanticAnalyzeException;
import org.gennai.gungnir.ql.stream.GroupedStream;
import org.gennai.gungnir.ql.stream.SingleStream;
import org.gennai.gungnir.ql.stream.Stream;
import org.gennai.gungnir.ql.stream.TupleJoinStreamBuilder.JoinToTupleDeclarer;
import org.gennai.gungnir.topology.processor.InMemoryTtlCacheProcessor;
import org.gennai.gungnir.topology.processor.Processor;
import org.gennai.gungnir.topology.processor.TtlCacheProcessor;
import org.gennai.gungnir.tuple.Condition;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.TupleAccessor;
import org.gennai.gungnir.tuple.schema.Schema;

import com.google.common.collect.Lists;

public class FromStreamsClauseAnalyzer {

  private FromClauseAnalyzer fromClauseAnalyzer;

  public FromStreamsClauseAnalyzer(FromClauseAnalyzer fromClauseAnalyzer) {
    this.fromClauseAnalyzer = fromClauseAnalyzer;
  }

  private TupleAccessor[] tupleNamesAnalyze(ASTNode node) throws SemanticAnalyzeException {
    if (node.getChildCount() == 0) {
      return null;
    }

    TupleAccessor[] tuples = new TupleAccessor[node.getChildCount()];
    for (int i = 0; i < node.getChildCount(); i++) {
      String tupleName =
          fromClauseAnalyzer.getSemanticAnalyzer().analyzeByAnalyzer(node.getChild(i));
      Schema schema = fromClauseAnalyzer.getSemanticAnalyzer().getSchemaRegistry().get(tupleName);
      if (schema != null) {
        tupleName = schema.getSchemaName();
      }
      tuples[i] = new TupleAccessor(tupleName);
    }
    return tuples;
  }

  private static class JoinStreamDeclare {

    private Stream joinStream;
    private Condition joinCondition;
  }

  private JoinStreamDeclare joinStreamExprAnalyze(ASTNode node, Stream stream)
      throws SemanticAnalyzeException, GungnirTopologyException {
    JoinStreamDeclare declare = new JoinStreamDeclare();

    String streamName =
        fromClauseAnalyzer.getSemanticAnalyzer().analyzeByAnalyzer(node.getChild(0));
    Stream s = fromClauseAnalyzer.getSemanticAnalyzer().getStream(streamName);
    if (s == null) {
      throw new SemanticAnalyzeException("Not exists stream '" + streamName + "'");
    }

    if (s.getClass() != stream.getClass()) {
      throw new SemanticAnalyzeException("Can't merge stream and grouped stream " + s);
    } else {
      if (s instanceof GroupedStream<?>) {
        if (!((GroupedStream<?>) s).getGroupFields().equals(
            ((GroupedStream<?>) stream).getGroupFields())) {
          throw new SemanticAnalyzeException("Can't merge stream grouped by different fields " + s);
        }
      }
    }

    String tupleName = fromClauseAnalyzer.getSemanticAnalyzer().analyzeByAnalyzer(node.getChild(1));
    declare.joinStream = s.select(new TupleAccessor(tupleName));

    Condition joinCondition =
        fromClauseAnalyzer.getSemanticAnalyzer().analyzeByAnalyzer(node.getChild(2));
    declare.joinCondition = joinCondition;

    return declare;
  }

  private List<JoinStreamDeclare> joinStreamExprsAnalyze(ASTNode node, Stream stream)
      throws SemanticAnalyzeException, GungnirTopologyException {
    List<JoinStreamDeclare> declares = Lists.newArrayListWithCapacity(node.getChildCount());
    for (int i = 0; i < node.getChildCount(); i++) {
      declares.add(joinStreamExprAnalyze(node.getChild(i), stream));
    }
    return declares;
  }

  private static class JoinStreamsDeclare {

    private Stream fromStream;
    private List<JoinStreamDeclare> streamDeclares;
    private FieldAccessor[] toFields;
    private TupleAccessor toTuple;
    private Period expire;
    private TtlCacheProcessor processor;
  }

  @SuppressWarnings("unchecked")
  public Stream analyze(ASTNode node) throws SemanticAnalyzeException, GungnirTopologyException {
    List<Stream> streams = Lists.newArrayList();
    List<JoinStreamsDeclare> streamsDeclares = null;
    Stream stream = null;

    for (int i = 0; i < node.getChild(0).getChildCount(); i++) {
      ASTNode child = node.getChild(0).getChild(i);

      String streamName =
          fromClauseAnalyzer.getSemanticAnalyzer().analyzeByAnalyzer(child.getChild(0));
      Stream s = fromClauseAnalyzer.getSemanticAnalyzer().getStream(streamName);
      if (s == null) {
        throw new SemanticAnalyzeException("Not exists stream '" + streamName + "'");
      }

      if (stream != null) {
        if (s.getClass() != stream.getClass()) {
          throw new SemanticAnalyzeException("Can't join stream and grouped stream " + stream);
        } else {
          if (s instanceof GroupedStream<?>) {
            if (!((GroupedStream<?>) s).getGroupFields().equals(
                ((GroupedStream<?>) stream).getGroupFields())) {
              throw new SemanticAnalyzeException("Can't merge stream grouped by different fields "
                  + s);
            }
          }
        }
      }
      stream = s;

      if (child.getChild(1) != null) {
        if (child.getChild(1).getType() == TOK_TUPLE_NAMES) {
          TupleAccessor[] tuples = tupleNamesAnalyze(child.getChild(1));
          if (tuples != null) {
            stream = stream.select(tuples);
          }
        } else {
          String tupleName =
              fromClauseAnalyzer.getSemanticAnalyzer().analyzeByAnalyzer(child.getChild(1));
          stream = stream.select(new TupleAccessor(tupleName));
        }
      }

      if (child.getChild(2) != null && child.getChild(2).getType() == TOK_JOIN_STREAM_EXPRS) {
        List<JoinStreamDeclare> declares = joinStreamExprsAnalyze(child.getChild(2), stream);

        JoinStreamsDeclare schemasDeclare = new JoinStreamsDeclare();
        schemasDeclare.fromStream = stream;
        schemasDeclare.streamDeclares = declares;
        schemasDeclare.toFields = fromClauseAnalyzer.tupleJoinToExprAnalyze(child.getChild(3));
        schemasDeclare.expire = fromClauseAnalyzer.getSemanticAnalyzer()
            .analyzeByAnalyzer(child.getChild(4));

        String toTupleName;
        if (child.getChild(5).getType() == TOK_PROCESSOR) {
          Processor p = fromClauseAnalyzer.getSemanticAnalyzer()
              .analyzeByAnalyzer(child.getChild(5));
          if (p instanceof TtlCacheProcessor) {
            schemasDeclare.processor = (TtlCacheProcessor) p;
          } else {
            throw new SemanticAnalyzeException("Processor isn't cache processor '" + p + "'");
          }

          toTupleName = fromClauseAnalyzer.getSemanticAnalyzer()
              .analyzeByAnalyzer(child.getChild(6));
        } else {
          schemasDeclare.processor = new InMemoryTtlCacheProcessor();

          toTupleName = fromClauseAnalyzer.getSemanticAnalyzer()
              .analyzeByAnalyzer(child.getChild(5));
        }
        schemasDeclare.toTuple = new TupleAccessor(toTupleName);
        fromClauseAnalyzer.getSemanticAnalyzer().getStreamTuples().add(toTupleName);

        if (streamsDeclares == null) {
          streamsDeclares = Lists.newArrayList();
        }
        streamsDeclares.add(schemasDeclare);
      } else {
        String aliasName =
            fromClauseAnalyzer.getSemanticAnalyzer().analyzeByAnalyzer(child.getChild(2));
        if (aliasName != null) {
          stream = stream.alias(new TupleAccessor(aliasName));
          fromClauseAnalyzer.getSemanticAnalyzer().getStreamTuples().add(aliasName);
        }
        streams.add(stream);
      }
    }

    if (streams.size() > 0) {
      stream =
          fromClauseAnalyzer.getSemanticAnalyzer().getTopology()
              .from(streams.toArray(new Stream[0]));
    }

    if (streamsDeclares == null) {
      return stream;
    } else {
      for (JoinStreamsDeclare streamsDeclare : streamsDeclares) {
        if (stream instanceof SingleStream) {
          JoinToTupleDeclarer<SingleStream> joinToTupleDeclarer =
              ((SingleStream) streamsDeclare.fromStream).join(
                  (SingleStream) streamsDeclare.streamDeclares.get(0).joinStream,
                  streamsDeclare.streamDeclares.get(0).joinCondition);
          for (int i = 1; i < streamsDeclare.streamDeclares.size(); i++) {
            joinToTupleDeclarer = joinToTupleDeclarer.join(
                ((SingleStream) streamsDeclare.streamDeclares.get(i).joinStream),
                streamsDeclare.streamDeclares.get(i).joinCondition);
          }
          SingleStream joinedStream =
              joinToTupleDeclarer.to(streamsDeclare.toTuple, streamsDeclare.toFields)
                  .expire(streamsDeclare.expire).using(streamsDeclare.processor).build();
          streams.add(joinedStream);
        } else {
          JoinToTupleDeclarer<GroupedStream<Stream>> joinToTupleDeclarer =
              ((GroupedStream<Stream>) streamsDeclare.fromStream).join(
                  (GroupedStream<Stream>) streamsDeclare.streamDeclares.get(0).joinStream,
                  streamsDeclare.streamDeclares.get(0).joinCondition);
          for (int i = 1; i < streamsDeclare.streamDeclares.size(); i++) {
            joinToTupleDeclarer = joinToTupleDeclarer.join(
                ((GroupedStream<Stream>) streamsDeclare.streamDeclares.get(0).joinStream),
                streamsDeclare.streamDeclares.get(0).joinCondition);
          }
          GroupedStream<Stream> joinedStream =
              joinToTupleDeclarer.to(streamsDeclare.toTuple, streamsDeclare.toFields)
                  .expire(streamsDeclare.expire).using(streamsDeclare.processor).build();
          streams.add(joinedStream);
        }
      }

      return fromClauseAnalyzer.getSemanticAnalyzer().getTopology()
          .from(streams.toArray(new Stream[0]));
    }
  }
}
