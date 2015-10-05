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

public class ConstantAnalyzer implements Analyzer<Object> {

  @Override
  public Object analyze(ASTNode node, SemanticAnalyzer semanticAnalyzer)
      throws SemanticAnalyzeException {
    String buff = node.getText();
    switch (node.getType()) {
      case StringLiteral:
        return buff.substring(1, buff.length() - 1);
      case TinyintLiteral:
        return Byte.valueOf(buff.substring(0, buff.length() - 1));
      case SmallintLiteral:
        return Short.valueOf(buff.substring(0, buff.length() - 1));
      case IntLiteral:
        return Integer.valueOf(node.getText());
      case BigintLiteral:
        return Long.valueOf(buff.substring(0, buff.length() - 1));
      case FloatLiteral:
        return Float.valueOf(buff.substring(0, buff.length() - 1));
      case DoubleLiteral:
        return Double.valueOf(node.getText());
      case TRUE:
        return Boolean.valueOf(node.getText());
      case FALSE:
        return Boolean.valueOf(node.getText());
      default:
        return node.getText();
    }
  }
}
