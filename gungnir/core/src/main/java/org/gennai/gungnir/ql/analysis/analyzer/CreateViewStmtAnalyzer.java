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

import org.gennai.gungnir.ql.analysis.ASTNode;
import org.gennai.gungnir.ql.analysis.SemanticAnalyzeException;
import org.gennai.gungnir.ql.analysis.SemanticAnalyzer;
import org.gennai.gungnir.ql.task.CreateSchemaTask;
import org.gennai.gungnir.tuple.Condition;
import org.gennai.gungnir.tuple.schema.Schema;
import org.gennai.gungnir.tuple.schema.TupleSchema;
import org.gennai.gungnir.tuple.schema.ViewSchema;

public class CreateViewStmtAnalyzer implements Analyzer<CreateSchemaTask> {

  @Override
  public CreateSchemaTask analyze(ASTNode node, SemanticAnalyzer semanticAnalyzer)
      throws SemanticAnalyzeException {
    String viewName = semanticAnalyzer.analyzeByAnalyzer(node.getChild(0));
    ViewSchema viewSchema = new ViewSchema(viewName);

    String tupleName = semanticAnalyzer.analyzeByAnalyzer(node.getChild(1));
    Schema tupleSchema = semanticAnalyzer.getSchemaRegistry().get(tupleName);
    if (tupleSchema == null || !(tupleSchema instanceof TupleSchema)) {
      throw new SemanticAnalyzeException(tupleName + " isn't registered");
    }
    viewSchema.from((TupleSchema) tupleSchema);

    Condition condition = semanticAnalyzer.analyzeByAnalyzer(node.getChild(2));
    viewSchema.filter(condition);

    viewSchema.setComment(semanticAnalyzer.<String>analyzeByAnalyzer(node.getChild(3)));

    viewSchema.setOwner(semanticAnalyzer.getOwner());
    return new CreateSchemaTask(viewSchema);
  }
}
