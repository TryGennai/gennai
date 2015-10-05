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
import org.gennai.gungnir.tuple.schema.FieldType;
import org.gennai.gungnir.tuple.schema.StructType;
import org.gennai.gungnir.tuple.schema.TupleSchema.FieldTypes;

public class FieldTypeAnalyzer implements Analyzer<FieldType> {

  private static void fieldDefineAnalyze(ASTNode node, SemanticAnalyzer semanticAnalyzer,
      StructType structType) throws SemanticAnalyzeException {
    String fieldName = semanticAnalyzer.analyzeByAnalyzer(node.getChild(0));
    FieldType fieldType = semanticAnalyzer.analyzeByAnalyzer(node.getChild(1));
    structType.field(fieldName, fieldType);
  }

  private static StructType fieldDefinesAnalyze(ASTNode node, SemanticAnalyzer semanticAnalyzer)
      throws SemanticAnalyzeException {
    StructType structType = FieldTypes.STRUCT();
    for (int i = 0; i < node.getChildCount(); i++) {
      fieldDefineAnalyze(node.getChild(i), semanticAnalyzer, structType);
    }
    return structType;
  }

  @Override
  public FieldType analyze(ASTNode node, SemanticAnalyzer semanticAnalyzer)
      throws SemanticAnalyzeException {
    switch (node.getType()) {
      case STRING:
        return FieldTypes.STRING;
      case TINYINT:
        return FieldTypes.TINYINT;
      case SMALLINT:
        return FieldTypes.SMALLINT;
      case INT:
        return FieldTypes.INT;
      case BIGINT:
        return FieldTypes.BIGINT;
      case FLOAT:
        return FieldTypes.FLOAT;
      case DOUBLE:
        return FieldTypes.DOUBLE;
      case BOOLEAN:
        return FieldTypes.BOOLEAN;
      case TIMESTAMP:
        if (node.getChild(0) == null) {
          return FieldTypes.TIMESTAMP;
        } else {
          String dateFormat = semanticAnalyzer.analyzeByAnalyzer(node.getChild(0));
          return FieldTypes.TIMESTAMP(dateFormat);
        }
      case LIST:
        FieldType elementType = semanticAnalyzer.analyzeByAnalyzer(node.getChild(0));
        return FieldTypes.LIST(elementType);
      case MAP:
        FieldType keyType = semanticAnalyzer.analyzeByAnalyzer(node.getChild(0));
        FieldType valueType = semanticAnalyzer.analyzeByAnalyzer(node.getChild(1));

        return FieldTypes.MAP(keyType, valueType);
      case STRUCT:
        return fieldDefinesAnalyze(node.getChild(0), semanticAnalyzer);
      default:
        throw new SemanticAnalyzeException("Unsupported type '" + node.getText() + "'");
    }
  }
}
