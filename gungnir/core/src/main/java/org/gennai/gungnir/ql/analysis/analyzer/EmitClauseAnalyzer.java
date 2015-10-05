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

import org.gennai.gungnir.GungnirTopologyException;
import org.gennai.gungnir.ql.analysis.ASTNode;
import org.gennai.gungnir.ql.analysis.SemanticAnalyzeException;
import org.gennai.gungnir.ql.analysis.SemanticAnalyzer;
import org.gennai.gungnir.ql.stream.GroupedStream;
import org.gennai.gungnir.ql.stream.Stream;
import org.gennai.gungnir.ql.stream.SingleStream;
import org.gennai.gungnir.topology.processor.EmitProcessor;
import org.gennai.gungnir.topology.processor.Processor;
import org.gennai.gungnir.topology.processor.SchemaPersistProcessor;
import org.gennai.gungnir.tuple.FieldAccessor;
import org.gennai.gungnir.tuple.schema.Schema;

public class EmitClauseAnalyzer {

  private SemanticAnalyzer semanticAnalyzer;

  public EmitClauseAnalyzer(SemanticAnalyzer semanticAnalyzer) {
    this.semanticAnalyzer = semanticAnalyzer;
  }

  public Stream analyze(ASTNode node, Stream stream)
      throws SemanticAnalyzeException, GungnirTopologyException {
    FieldAccessor[] fields = semanticAnalyzer.analyzeByAnalyzer(node.getChild(0));

    EmitProcessor processor = null;
    switch (node.getChild(1).getType()) {
      case TOK_PROCESSOR:
        Processor p = semanticAnalyzer.analyzeByAnalyzer(node.getChild(1));
        if (p instanceof EmitProcessor) {
          processor = (EmitProcessor) p;
        } else {
          throw new SemanticAnalyzeException("Processor isn't emit processor '" + p + "'");
        }
        break;
      case Identifier:
        String schemaName = semanticAnalyzer.analyzeByAnalyzer(node.getChild(1));
        Schema schema = semanticAnalyzer.getSchemaRegistry().get(schemaName);
        if (schema == null) {
          throw new SemanticAnalyzeException(schemaName + " isn't registered");
        }
        processor = new SchemaPersistProcessor(schema);
        break;
      default:
        throw new SemanticAnalyzeException("Invalid emit clause '" + node.getChild(1).getText()
            + "'");
    }

    Integer parallelism = semanticAnalyzer.analyzeByAnalyzer(node.getChild(2));

    if (stream instanceof SingleStream) {
      return ((SingleStream) stream).emit(processor, fields).parallelism(parallelism);
    } else {
      return ((GroupedStream<?>) stream).emit(processor, fields).parallelism(parallelism);
    }
  }
}
