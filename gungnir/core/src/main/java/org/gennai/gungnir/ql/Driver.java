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

package org.gennai.gungnir.ql;

import java.util.Map;

import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.TokenRewriteStream;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.ql.analysis.ANTLRNoCaseStringStream;
import org.gennai.gungnir.ql.analysis.ASTAdaptor;
import org.gennai.gungnir.ql.analysis.ASTNode;
import org.gennai.gungnir.ql.analysis.GungnirLexer;
import org.gennai.gungnir.ql.analysis.GungnirParser;
import org.gennai.gungnir.ql.analysis.ParseException;
import org.gennai.gungnir.ql.analysis.SemanticAnalyzeException;
import org.gennai.gungnir.ql.analysis.SemanticAnalyzer;
import org.gennai.gungnir.ql.session.StatementEntity;
import org.gennai.gungnir.ql.task.TaskExecuteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Driver implements CommandProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(Driver.class);

  private SemanticAnalyzer semanticAnalyzer;

  @Override
  public boolean canRun(String command) {
    return true;
  }

  private void compile(StatementEntity statement, String command)
      throws ParseException, SemanticAnalyzeException, MetaStoreException {
    if (semanticAnalyzer == null) {
      semanticAnalyzer = new SemanticAnalyzer(statement);
    } else {
      semanticAnalyzer.setStatement(statement);
    }

    semanticAnalyzer.getSchemaRegistry().load(statement.getOwner());
    Map<String, String> aliasNamesMap = statement.getAliasNamesMap();
    if (aliasNamesMap != null) {
      for (Map.Entry<String, String> entry : aliasNamesMap.entrySet()) {
        semanticAnalyzer.getSchemaRegistry().register(entry.getKey(), entry.getValue());
      }
    }

    semanticAnalyzer.getFunctionRegistry().load(statement.getOwner());

    GungnirLexer lexer = new GungnirLexer(new ANTLRNoCaseStringStream(command));
    TokenRewriteStream tokens = new TokenRewriteStream(lexer);
    GungnirParser parser = new GungnirParser(tokens);
    parser.setTreeAdaptor(new ASTAdaptor());
    GungnirParser.statement_return r = null;
    try {
      r = parser.statement();
    } catch (RecognitionException e) {
      throw new ParseException(parser.getErrors());
    }

    if (lexer.getErrors().size() == 0 && parser.getErrors().size() == 0) {
      LOG.info("Parse completed");
    } else if (lexer.getErrors().size() != 0) {
      throw new ParseException(lexer.getErrors());
    } else {
      throw new ParseException(parser.getErrors());
    }

    ASTNode ast = (ASTNode) r.getTree();

    LOG.info("{}\n", ast.getChild(0).dump());

    semanticAnalyzer.analyze(ast.getChild(0));
  }

  private String execute() throws TaskExecuteException {
    if (semanticAnalyzer.getTask() == null) {
      return "OK";
    }
    return semanticAnalyzer.getTask().execute();
  }

  @Override
  public String run(StatementEntity statement, String command) throws CommandProcessorException {
    try {
      compile(statement, command);
      return execute();
    } catch (ParseException e) {
      throw new CommandProcessorException(e);
    } catch (SemanticAnalyzeException e) {
      throw new CommandProcessorException(e);
    } catch (MetaStoreException e) {
      throw new CommandProcessorException(e);
    } catch (TaskExecuteException e) {
      if (e.getCause() != null) {
        throw new CommandProcessorException(e.getCause());
      } else {
        throw new CommandProcessorException(e);
      }
    }
  }

  @Override
  public Driver clone() {
    return new Driver();
  }
}
