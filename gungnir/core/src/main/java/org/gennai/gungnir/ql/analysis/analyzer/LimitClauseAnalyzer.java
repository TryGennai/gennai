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
import org.gennai.gungnir.topology.operator.LimitOperator.LimitType;
import org.gennai.gungnir.topology.operator.limit.LimitInterval;

public class LimitClauseAnalyzer {

  private SemanticAnalyzer semanticAnalyzer;

  public LimitClauseAnalyzer(SemanticAnalyzer semanticAnalyzer) {
    this.semanticAnalyzer = semanticAnalyzer;
  }

  private Stream limitClauseAnalyze(LimitType type, ASTNode node, Stream stream)
      throws SemanticAnalyzeException, GungnirTopologyException {
    LimitInterval interval = null;
    switch (node.getChild(0).getType()) {
      case TOK_COUNT_INTERVAL:
        Integer count = semanticAnalyzer.analyzeByAnalyzer(node.getChild(0));
        interval = LimitInterval.count(count);
        break;
      case TOK_TIME_INTERVAL:
        Period period = semanticAnalyzer.analyzeByAnalyzer(node.getChild(0));
        interval = LimitInterval.time(period);
        break;
      default:
        throw new SemanticAnalyzeException("Invalid limit interval '" + node.getChild(0).getText()
            + "'");
    }

    if (stream instanceof SingleStream) {
      return ((SingleStream) stream).limit(type, interval);
    } else {
      return ((GroupedStream<?>) stream).limit(type, interval);
    }
  }

  public Stream analyze(ASTNode node, Stream stream)
      throws SemanticAnalyzeException, GungnirTopologyException {
    Stream s = null;
    switch (node.getChild(0).getType()) {
      case TOK_FIRST:
        s = limitClauseAnalyze(LimitType.FIRST, node.getChild(0), stream);
        break;
      case TOK_LAST:
        s = limitClauseAnalyze(LimitType.LAST, node.getChild(0), stream);
        break;
      default:
        throw new SemanticAnalyzeException("Invalid limit caluse '" + node.getChild(0).getText()
            + "'");
    }

    Integer parallelism = semanticAnalyzer.analyzeByAnalyzer(node.getChild(1));

    if (stream instanceof SingleStream) {
      return ((SingleStream) s).parallelism(parallelism);
    } else {
      return ((GroupedStream<?>) s).parallelism(parallelism);
    }
  }
}
