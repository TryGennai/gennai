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
import org.gennai.gungnir.ql.analysis.ASTNode;
import org.gennai.gungnir.ql.analysis.SemanticAnalyzeException;
import org.gennai.gungnir.ql.analysis.SemanticAnalyzer;
import org.gennai.gungnir.ql.stream.Stream;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.TupleAccessor;

public class FromClauseAnalyzer {

  private SemanticAnalyzer semanticAnalyzer;

  public FromClauseAnalyzer(SemanticAnalyzer semanticAnalyzer) {
    this.semanticAnalyzer = semanticAnalyzer;
  }

  public SemanticAnalyzer getSemanticAnalyzer() {
    return semanticAnalyzer;
  }

  private FieldAccessor tupleJoinFieldAnalyze(ASTNode node) throws SemanticAnalyzeException {
    String tupleName = semanticAnalyzer.analyzeByAnalyzer(node.getChild(0));

    String fieldName = null;
    switch (node.getChild(1).getType()) {
      case Identifier:
        fieldName = semanticAnalyzer.analyzeByAnalyzer(node.getChild(1));
        break;
      case ASTERISK:
        fieldName = "*";
        break;
      default:
        throw new SemanticAnalyzeException("Invalid join field format '"
            + node.getChild(1).getText() + "'");
    }

    String aliasFieldName = semanticAnalyzer.analyzeByAnalyzer(node.getChild(2));

    return new TupleAccessor(tupleName).field(fieldName).as(aliasFieldName);
  }

  FieldAccessor[] tupleJoinToExprAnalyze(ASTNode node) throws SemanticAnalyzeException {

    FieldAccessor[] fields = new FieldAccessor[node.getChildCount()];
    for (int i = 0; i < node.getChildCount(); i++) {
      fields[i] = tupleJoinFieldAnalyze(node.getChild(i));
    }
    return fields;
  }

  public Stream analyze(ASTNode node)
      throws SemanticAnalyzeException, GungnirTopologyException {
    switch (node.getChild(0).getType()) {
      case TOK_SPOUT:
        semanticAnalyzer.clear();
        return new FromSpoutClauseAnalyzer(this).analyze(node.getChild(0));
      case TOK_STREAM:
        return new FromStreamsClauseAnalyzer(this).analyze(node.getChild(0));
      default:
        throw new SemanticAnalyzeException("Invalid from clause '" + node.getChild(0).getText()
            + "'");
    }
  }
}
