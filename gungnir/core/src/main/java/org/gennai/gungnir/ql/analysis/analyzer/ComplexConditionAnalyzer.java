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

import org.gennai.gungnir.ql.analysis.ASTNode;
import org.gennai.gungnir.ql.analysis.SemanticAnalyzeException;
import org.gennai.gungnir.ql.analysis.SemanticAnalyzer;
import org.gennai.gungnir.tuple.ComplexCondition;
import org.gennai.gungnir.tuple.Condition;

public class ComplexConditionAnalyzer implements Analyzer<ComplexCondition> {

  @Override
  public ComplexCondition analyze(ASTNode node, SemanticAnalyzer semanticAnalyzer)
      throws SemanticAnalyzeException {
    ComplexCondition.Type type = null;
    switch (node.getType()) {
      case TOK_OP_OR:
        type = ComplexCondition.Type.OR;
        break;
      case TOK_OP_AND:
        type = ComplexCondition.Type.AND;
        break;
      case TOK_OP_NOT:
        type = ComplexCondition.Type.NOT;
        break;
      default:
        throw new SemanticAnalyzeException("Invalid condition type '" + node.getText() + "'");
    }

    Condition[] conditions = new Condition[node.getChildCount()];
    for (int i = 0; i < node.getChildCount(); i++) {
      conditions[i] = semanticAnalyzer.analyzeByAnalyzer(node.getChild(i));
    }
    return new ComplexCondition(type, conditions);
  }
}
