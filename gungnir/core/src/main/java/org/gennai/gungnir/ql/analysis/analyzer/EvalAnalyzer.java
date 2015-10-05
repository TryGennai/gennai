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
import org.gennai.gungnir.topology.ArithNode;
import org.gennai.gungnir.topology.ArithNode.Operator;
import org.gennai.gungnir.topology.FieldArithNode;
import org.gennai.gungnir.topology.InternalArithNode;
import org.gennai.gungnir.topology.NumberArithNode;
import org.gennai.gungnir.topology.udf.Eval;
import org.gennai.gungnir.topology.udf.Function;
import org.gennai.gungnir.tuple.Field;

public class EvalAnalyzer implements Analyzer<Function<?>> {

  private ArithNode analyzeArithNode(ASTNode node, SemanticAnalyzer semanticAnalyzer)
      throws SemanticAnalyzeException {
    Operator operator;
    switch (node.getType()) {
      case PLUS:
        operator = Operator.ADDITION;
        break;
      case MINUS:
        operator = Operator.SUBTRACTION;
        break;
      case ASTERISK:
        operator = Operator.MULTIPLICATION;
        break;
      case SLASH:
        operator = Operator.DIVISION;
        break;
      case PERCENT:
        operator = Operator.MODULO;
        break;
      case DIV:
        operator = Operator.INTEGER_DIVISION;
        break;
      case MOD:
        operator = Operator.MODULO;
        break;
      case TOK_FIELD:
      case TOK_FUNCTION:
      case TOK_CAST:
        return new FieldArithNode(semanticAnalyzer.<Field>analyzeByAnalyzer(node));
      default:
        return new NumberArithNode(semanticAnalyzer.<Number>analyzeByAnalyzer(node));
    }

    ArithNode leftNode = analyzeArithNode(node.getChild(0), semanticAnalyzer);
    ArithNode rightNode = analyzeArithNode(node.getChild(1), semanticAnalyzer);
    return new InternalArithNode(operator, leftNode, rightNode);
  }

  @Override
  public Function<?> analyze(ASTNode node, SemanticAnalyzer semanticAnalyzer)
      throws SemanticAnalyzeException {
    InternalArithNode arithTree =
        (InternalArithNode) analyzeArithNode(node, semanticAnalyzer);

    Eval func = null;
    try {
      func = new Eval().create(arithTree);
    } catch (Exception e) {
      throw new SemanticAnalyzeException("Faild to create 'eval' function", e);
    }

    return func;
  }
}
