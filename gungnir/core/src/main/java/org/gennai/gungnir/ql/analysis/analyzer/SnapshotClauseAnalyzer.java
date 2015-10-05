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

import org.gennai.gungnir.GungnirTopologyException;
import org.gennai.gungnir.Period;
import org.gennai.gungnir.ql.analysis.ASTNode;
import org.gennai.gungnir.ql.analysis.SemanticAnalyzeException;
import org.gennai.gungnir.ql.analysis.SemanticAnalyzer;
import org.gennai.gungnir.ql.stream.GroupedStream;
import org.gennai.gungnir.ql.stream.Stream;
import org.gennai.gungnir.ql.stream.SingleStream;
import org.gennai.gungnir.topology.operator.snapshot.SnapshotInterval;
import org.gennai.gungnir.tuple.Field;

public class SnapshotClauseAnalyzer {

  private SemanticAnalyzer semanticAnalyzer;

  public SnapshotClauseAnalyzer(SemanticAnalyzer semanticAnalyzer) {
    this.semanticAnalyzer = semanticAnalyzer;
  }

  private SnapshotInterval intervalAnalyze(ASTNode node) throws SemanticAnalyzeException {
    switch (node.getType()) {
      case TOK_COUNT_INTERVAL:
        Integer count = semanticAnalyzer.analyzeByAnalyzer(node);
        return SnapshotInterval.count(count);
      case TOK_TIME_INTERVAL:
        Period period = semanticAnalyzer.analyzeByAnalyzer(node);
        return SnapshotInterval.time(period);
      case TOK_CRON_INTERVAL:
        String schedulingPattern = semanticAnalyzer.analyzeByAnalyzer(node);
        SnapshotInterval interval = SnapshotInterval.cron(schedulingPattern);
        if (!interval.validateSchedulingPattern()) {
          throw new SemanticAnalyzeException("Invalid snapshot interval '" + interval + "'");
        }
        return interval;
      default:
        throw new SemanticAnalyzeException("Invalid snaphost interval '" + node.getText() + "'");
    }
  }

  public Stream analyze(ASTNode node, Stream stream)
      throws SemanticAnalyzeException, GungnirTopologyException {
    SnapshotInterval interval = intervalAnalyze(node.getChild(0));

    Field[] exprs = semanticAnalyzer.analyzeByAnalyzer(node.getChild(1));

    SnapshotInterval expire = null;
    Integer parallelism = null;
    if (node.getChild(2) != null) {
      if (node.getChild(2).getType() == TOK_COUNT_INTERVAL
          || node.getChild(2).getType() == TOK_TIME_INTERVAL
          || node.getChild(2).getType() == TOK_CRON_INTERVAL) {
        expire = intervalAnalyze(node.getChild(2));

        parallelism = semanticAnalyzer.analyzeByAnalyzer(node.getChild(3));
      } else {
        parallelism = semanticAnalyzer.analyzeByAnalyzer(node.getChild(2));
      }
    }

    if (stream instanceof SingleStream) {
      return ((SingleStream) stream).snapshot(interval, expire, exprs).parallelism(parallelism);
    } else {
      return ((GroupedStream<?>) stream).snapshot(interval, expire, exprs).parallelism(parallelism);
    }
  }
}
