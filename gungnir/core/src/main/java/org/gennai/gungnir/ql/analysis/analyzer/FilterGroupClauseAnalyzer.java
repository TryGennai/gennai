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
import org.gennai.gungnir.tuple.Condition;
import org.gennai.gungnir.tuple.FieldAccessor;

public class FilterGroupClauseAnalyzer {

  private SemanticAnalyzer semanticAnalyzer;

  public FilterGroupClauseAnalyzer(SemanticAnalyzer semanticAnalyzer) {
    this.semanticAnalyzer = semanticAnalyzer;
  }

  private Condition[] conditionsAnalyze(ASTNode node) throws SemanticAnalyzeException {
    Condition[] conditions = new Condition[node.getChildCount()];
    for (int i = 0; i < node.getChildCount(); i++) {
      conditions[i] = semanticAnalyzer.analyzeByAnalyzer(node.getChild(i));
    }
    return conditions;
  }

  public Stream analyze(ASTNode node, Stream stream)
      throws SemanticAnalyzeException, GungnirTopologyException {
    Period expire = semanticAnalyzer.analyzeByAnalyzer(node.getChild(0));

    FieldAccessor stateField = null;
    Condition[] conditions = null;
    Integer parallelism = null;
    if (node.getChild(1).getType() == Identifier) {
      String fieldName = semanticAnalyzer.analyzeByAnalyzer(node.getChild(1));
      stateField = new FieldAccessor(fieldName);
      conditions = conditionsAnalyze(node.getChild(2));
      parallelism = semanticAnalyzer.analyzeByAnalyzer(node.getChild(3));
    } else {
      conditions = conditionsAnalyze(node.getChild(1));
      parallelism = semanticAnalyzer.analyzeByAnalyzer(node.getChild(2));
    }

    if (stream instanceof SingleStream) {
      return ((SingleStream) stream).filterGroup(expire, stateField, conditions).parallelism(
          parallelism);
    } else {
      return ((GroupedStream<?>) stream).filterGroup(expire, stateField, conditions)
          .parallelism(parallelism);
    }
  }
}
