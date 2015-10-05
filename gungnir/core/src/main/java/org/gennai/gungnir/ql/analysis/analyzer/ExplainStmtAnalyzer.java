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
import org.gennai.gungnir.ql.task.ExplainTask;

public class ExplainStmtAnalyzer implements Analyzer<ExplainTask> {

  @Override
  public ExplainTask analyze(ASTNode node, SemanticAnalyzer semanticAnalyzer)
      throws SemanticAnalyzeException {
    boolean extended = false;
    String topologyName = null;
    if (node.getChildCount() > 0) {
      if (node.getChild(0).getType() == TOK_EXTENDED) {
        extended = true;
        topologyName = semanticAnalyzer.analyzeByAnalyzer(node.getChild(1));
      } else {
        topologyName = semanticAnalyzer.analyzeByAnalyzer(node.getChild(0));
      }
    }

    if (topologyName == null) {
      if (semanticAnalyzer.getTopology() == null) {
        throw new SemanticAnalyzeException("Topology isn't registered");
      }
      return new ExplainTask(semanticAnalyzer.getTopology(), extended);
    } else {
      return new ExplainTask(topologyName, semanticAnalyzer.getOwner(), extended);
    }
  }
}
