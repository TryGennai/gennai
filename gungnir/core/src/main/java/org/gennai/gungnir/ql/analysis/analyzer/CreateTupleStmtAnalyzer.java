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
import org.gennai.gungnir.ql.task.CreateSchemaTask;
import org.gennai.gungnir.tuple.schema.FieldType;
import org.gennai.gungnir.tuple.schema.TupleSchema;

public class CreateTupleStmtAnalyzer implements Analyzer<CreateSchemaTask> {

  private static void fieldDefineAnalyze(ASTNode node, SemanticAnalyzer semanticAnalyzer,
      TupleSchema tupleSchema) throws SemanticAnalyzeException {
    String fieldName = semanticAnalyzer.analyzeByAnalyzer(node.getChild(0));
    FieldType fieldType = semanticAnalyzer.analyzeByAnalyzer(node.getChild(1));
    tupleSchema.field(fieldName, fieldType);
  }

  private static void fieldDefinesAnalyze(ASTNode node, SemanticAnalyzer semanticAnalyzer,
      TupleSchema tupleSchema) throws SemanticAnalyzeException {
    for (int i = 0; i < node.getChildCount(); i++) {
      fieldDefineAnalyze(node.getChild(i), semanticAnalyzer, tupleSchema);
    }
  }

  private static String[] partitionedByExprAnalyze(ASTNode node, SemanticAnalyzer semanticAnalyzer)
      throws SemanticAnalyzeException {
    if (node == null || node.getType() != TOK_PARTITIONED_BY) {
      return null;
    }

    String[] fieldNames = new String[node.getChildCount()];
    for (int i = 0; i < node.getChildCount(); i++) {
      fieldNames[i] = semanticAnalyzer.analyzeByAnalyzer(node.getChild(i));
    }
    return fieldNames;
  }

  @Override
  public CreateSchemaTask analyze(ASTNode node, SemanticAnalyzer semanticAnalyzer)
      throws SemanticAnalyzeException {
    String tupleName = semanticAnalyzer.analyzeByAnalyzer(node.getChild(0));
    TupleSchema tupleSchema = new TupleSchema(tupleName);

    fieldDefinesAnalyze(node.getChild(1), semanticAnalyzer, tupleSchema);

    if (node.getChildCount() > 2) {
      if (node.getChild(2).getType() == TOK_PARTITIONED_BY) {
        tupleSchema.partitioned(partitionedByExprAnalyze(node.getChild(2), semanticAnalyzer));
        tupleSchema.setComment(semanticAnalyzer.<String>analyzeByAnalyzer(node.getChild(3)));
      } else {
        tupleSchema.setComment(semanticAnalyzer.<String>analyzeByAnalyzer(node.getChild(2)));
      }
    }

    tupleSchema.setOwner(semanticAnalyzer.getOwner());
    return new CreateSchemaTask(tupleSchema);
  }
}
