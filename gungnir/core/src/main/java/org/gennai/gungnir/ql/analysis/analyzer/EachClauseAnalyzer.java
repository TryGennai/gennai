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
import org.gennai.gungnir.ql.stream.GroupedStream;
import org.gennai.gungnir.ql.stream.Stream;
import org.gennai.gungnir.ql.stream.SingleStream;
import org.gennai.gungnir.topology.udf.Function;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.Field;

public class EachClauseAnalyzer {

  private SemanticAnalyzer semanticAnalyzer;

  public EachClauseAnalyzer(SemanticAnalyzer semanticAnalyzer) {
    this.semanticAnalyzer = semanticAnalyzer;
  }

  private Field eachExprAnalyze(ASTNode node) throws SemanticAnalyzeException {
    Field field = semanticAnalyzer.analyzeByAnalyzer(node.getChild(0));
    String aliasFieldName = semanticAnalyzer.analyzeByAnalyzer(node.getChild(1));

    if (aliasFieldName != null) {
      if (field instanceof FieldAccessor) {
        ((FieldAccessor) field).as(aliasFieldName);
      } else {
        ((Function<?>) field).as(aliasFieldName);
      }
    }

    return field;
  }

  private Field[] eachExprsAnalyze(ASTNode node) throws SemanticAnalyzeException {
    Field[] exprs = new Field[node.getChildCount()];
    for (int i = 0; i < node.getChildCount(); i++) {
      exprs[i] = eachExprAnalyze(node.getChild(i));
    }
    return exprs;
  }

  public Stream analyze(ASTNode node, Stream stream)
      throws SemanticAnalyzeException, GungnirTopologyException {
    Field[] exprs = eachExprsAnalyze(node.getChild(0));

    Integer parallelism = semanticAnalyzer.analyzeByAnalyzer(node.getChild(1));

    if (stream instanceof SingleStream) {
      return ((SingleStream) stream).each(exprs).parallelism(parallelism);
    } else {
      return ((GroupedStream<?>) stream).each(exprs).parallelism(parallelism);
    }
  }
}
