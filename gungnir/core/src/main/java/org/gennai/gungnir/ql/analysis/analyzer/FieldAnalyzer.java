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
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.TupleAccessor;
import org.gennai.gungnir.tuple.schema.Schema;

public class FieldAnalyzer implements Analyzer<FieldAccessor> {

  private static void subscriptsAnalyze(ASTNode node, SemanticAnalyzer semanticAnalyzer,
      FieldAccessor field) throws SemanticAnalyzeException {
    for (int i = 0; i < node.getChildCount(); i++) {
      field.select(semanticAnalyzer.analyzeByAnalyzer(node.getChild(i)));
    }
  }

  @Override
  public FieldAccessor analyze(ASTNode node, SemanticAnalyzer semanticAnalyzer)
      throws SemanticAnalyzeException {
    TupleAccessor tuple = null;
    FieldAccessor field = null;

    for (int i = 0; i < node.getChildCount(); i++) {
      switch (node.getChild(i).getType()) {
        case Identifier:
          String name = semanticAnalyzer.analyzeByAnalyzer(node.getChild(i));
          if (tuple == null && field == null) {
            Schema schema = semanticAnalyzer.getSchemaRegistry().get(name);
            if (schema != null) {
              tuple = new TupleAccessor(schema.getSchemaName());
            } else if (semanticAnalyzer.getStreamTuples().contains(name)) {
              tuple = new TupleAccessor(name);
            } else {
              field = new FieldAccessor(name);
            }
          } else {
            if (field == null) {
              field = tuple.field(name);
            } else {
              field = field.field(name);
            }
          }
          break;
        case ASTERISK:
          if (field == null) {
            if (tuple != null) {
              field = tuple.field("*");
            } else {
              field = new FieldAccessor("*");
            }
          } else {
            field = field.field("*");
          }
          break;
        case TOK_SUBSCRIPT:
          subscriptsAnalyze(node.getChild(i), semanticAnalyzer, field);
          break;
        default:
          throw new SemanticAnalyzeException("Invalid field format '" + node.getChild(i).getText()
              + "'");
      }
    }

    return field;
  }
}
