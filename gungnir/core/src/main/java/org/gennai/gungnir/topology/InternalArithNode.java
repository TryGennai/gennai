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

package org.gennai.gungnir.topology;

import static org.gennai.gungnir.GungnirConst.*;

import java.util.List;
import java.util.Set;

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.tuple.FieldAccessor;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class InternalArithNode implements ArithNode {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private ArithNode leftNode;
  private ArithNode rightNode;
  private Operator operatior = Operator.NONE;

  public InternalArithNode(Operator operator, ArithNode leftNode,
      ArithNode rightNode) {
    this.operatior = operator;
    this.leftNode = leftNode;
    this.rightNode = rightNode;
  }

  public Operator getOperatior() {
    return operatior;
  }

  public ArithNode getLeftNode() {
    return leftNode;
  }

  public ArithNode getRightNode() {
    return rightNode;
  }

  public void prepare(GungnirConfig config, GungnirContext context) {
    if (leftNode instanceof InternalArithNode) {
      ((InternalArithNode) leftNode).prepare(config, context);
    } else if (leftNode instanceof FieldArithNode) {
      ((FieldArithNode) leftNode).prepare(config, context);
    }
    if (rightNode instanceof InternalArithNode) {
      ((InternalArithNode) rightNode).prepare(config, context);
    } else if (rightNode instanceof FieldArithNode) {
      ((FieldArithNode) rightNode).prepare(config, context);
    }
  }

  @Override
  public List<FieldAccessor> getFields() {
    Set<FieldAccessor> fields = Sets.newLinkedHashSet();
    List<FieldAccessor> leftFields = leftNode.getFields();
    List<FieldAccessor> rightFields = rightNode.getFields();
    if (leftFields != null) {
      fields.addAll(leftFields);
    }
    if (rightFields != null) {
      fields.addAll(rightFields);
    }
    return Lists.newArrayList(fields);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    if (leftNode instanceof InternalArithNode) {
      sb.append('(');
    }
    sb.append(leftNode);
    if (leftNode instanceof InternalArithNode) {
      sb.append(')');
    }
    sb.append(' ');
    sb.append(operatior.getDisplayString());
    sb.append(' ');
    if (rightNode instanceof InternalArithNode) {
      sb.append('(');
    }
    sb.append(rightNode);
    if (rightNode instanceof InternalArithNode) {
      sb.append(')');
    }
    return sb.toString();
  }
}
