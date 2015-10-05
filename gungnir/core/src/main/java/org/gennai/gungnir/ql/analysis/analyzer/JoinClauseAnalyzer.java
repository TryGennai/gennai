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

import org.gennai.gungnir.GungnirTopologyException;
import org.gennai.gungnir.ql.analysis.ASTNode;
import org.gennai.gungnir.ql.analysis.SemanticAnalyzeException;
import org.gennai.gungnir.ql.analysis.SemanticAnalyzer;
import org.gennai.gungnir.ql.stream.SingleStream;
import org.gennai.gungnir.ql.stream.Stream;
import org.gennai.gungnir.topology.processor.FetchProcessor;
import org.gennai.gungnir.topology.processor.Processor;

public class JoinClauseAnalyzer {

  private SemanticAnalyzer semanticAnalyzer;

  public JoinClauseAnalyzer(SemanticAnalyzer semanticAnalyzer) {
    this.semanticAnalyzer = semanticAnalyzer;
  }

  private String[] joinToExprAnalyze(ASTNode node) throws SemanticAnalyzeException {
    String[] fields = new String[node.getChildCount()];
    for (int i = 0; i < node.getChildCount(); i++) {
      fields[i] = semanticAnalyzer.analyzeByAnalyzer(node.getChild(i));
    }
    return fields;
  }

  private FetchProcessor processorAnalyze(ASTNode node) throws SemanticAnalyzeException {
    Processor p = semanticAnalyzer.analyzeByAnalyzer(node);
    if (p instanceof FetchProcessor) {
      return (FetchProcessor) p;
    } else {
      throw new SemanticAnalyzeException("Processor isn't fetch processor '" + p + "'");
    }
  }

  public Stream analyze(ASTNode node, Stream stream)
      throws SemanticAnalyzeException, GungnirTopologyException {
    String[] toFieldNames = joinToExprAnalyze(node.getChild(0));
    FetchProcessor processor = processorAnalyze(node.getChild(1));
    Integer parallelism = semanticAnalyzer.analyzeByAnalyzer(node.getChild(2));

    if (stream instanceof SingleStream) {
      return ((SingleStream) stream).join(processor, toFieldNames).parallelism(parallelism);
    }
    return stream;
  }
}
