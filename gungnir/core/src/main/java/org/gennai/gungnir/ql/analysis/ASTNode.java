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

package org.gennai.gungnir.ql.analysis;

import java.util.List;

import org.antlr.runtime.Token;
import org.antlr.runtime.tree.CommonTree;

import com.google.common.collect.Lists;

public class ASTNode extends CommonTree implements Node {

  public ASTNode(Token t) {
    super(t);
  }

  @Override
  public ASTNode getChild(int i) {
    return (ASTNode) super.getChild(i);
  }

  @Override
  public List<ASTNode> getChildren() {
    List<ASTNode> children = Lists.newArrayListWithCapacity(getChildCount());
    for (int i = 0; i < getChildCount(); i++) {
      children.add((ASTNode) super.getChildren());
    }
    return children;
  }

  @Override
  public String getName() {
    return (Integer.valueOf(super.getToken().getType())).toString();
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append('(');
    sb.append(super.toString());
    for (int i = 0; i < getChildCount(); i++) {
      sb.append(getChild(i).toString());
    }
    sb.append(')');
    return sb.toString();
  }

  private String dump(String indent, boolean isLast) {
    StringBuilder sb = new StringBuilder();
    sb.append(indent);
    sb.append(isLast ? "└ " : "├ ");
    sb.append(super.toString());
    if (token != null) {
      sb.append(" (");
      sb.append(token.getType());
      sb.append(')');
    }
    sb.append('\n');

    for (int i = 0; i < getChildCount(); i++) {
      sb.append(getChild(i).dump(indent + (isLast ? "   " : "│  "), i == getChildCount() - 1));
    }

    if (indent.isEmpty()) {
      sb.deleteCharAt(sb.length() - 1);
    }
    return sb.toString();
  }

  public String dump() {
    return dump("", true);
  }
}
