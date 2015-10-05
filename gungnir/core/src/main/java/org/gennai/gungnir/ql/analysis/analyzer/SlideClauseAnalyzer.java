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
import org.gennai.gungnir.topology.operator.slide.SlideLength;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.Field;

public class SlideClauseAnalyzer {

  private SemanticAnalyzer semanticAnalyzer;

  public SlideClauseAnalyzer(SemanticAnalyzer semanticAnalyzer) {
    this.semanticAnalyzer = semanticAnalyzer;
  }

  public Stream analyze(ASTNode node, Stream stream)
      throws SemanticAnalyzeException, GungnirTopologyException {
    SlideLength slideLength = null;
    Field[] exprs = null;
    Integer parallelism = null;

    switch (node.getChild(0).getType()) {
      case TOK_COUNT_INTERVAL:
        Integer count = semanticAnalyzer.analyzeByAnalyzer(node.getChild(0));
        slideLength = SlideLength.count(count);
        exprs = semanticAnalyzer.analyzeByAnalyzer(node.getChild(1));
        parallelism = semanticAnalyzer.analyzeByAnalyzer(node.getChild(2));
        break;
      case TOK_TIME_INTERVAL:
        Period period = semanticAnalyzer.analyzeByAnalyzer(node.getChild(0));
        FieldAccessor timeField = semanticAnalyzer.analyzeByAnalyzer(node.getChild(1));
        slideLength = SlideLength.time(period, timeField);
        exprs = semanticAnalyzer.analyzeByAnalyzer(node.getChild(2));
        parallelism = semanticAnalyzer.analyzeByAnalyzer(node.getChild(3));
        break;
      default:
        throw new SemanticAnalyzeException("Invalid slide interval '" + node.getChild(0).getText()
            + "'");
    }

    if (stream instanceof SingleStream) {
      return ((SingleStream) stream).slide(slideLength, exprs).parallelism(parallelism);
    } else {
      return ((GroupedStream<?>) stream).slide(slideLength, exprs).parallelism(parallelism);
    }
  }
}
