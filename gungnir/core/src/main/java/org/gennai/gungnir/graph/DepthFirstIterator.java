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

package org.gennai.gungnir.graph;

import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.traverse.AbstractGraphIterator;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class DepthFirstIterator<V, E> extends AbstractGraphIterator<V, E> {

  private enum VisitColor {
    WHITE, GRAY, BLACK
  }

  public static final Object SENTINEL = new Object();

  private final DefaultDirectedGraph<V, E> graph;
  private Iterator<V> vertexIterator = null;
  private V startVertex;
  private Deque<Object> stack = new ArrayDeque<Object>();
  private Map<V, VisitColor> seen = Maps.newHashMap();

  public DepthFirstIterator(DefaultDirectedGraph<V, E> graph) {
    super();

    this.graph = graph;

    vertexIterator = graph.vertexSet().iterator();
    setCrossComponentTraversal(startVertex == null);

    if (vertexIterator.hasNext()) {
      startVertex = vertexIterator.next();
    } else {
      startVertex = null;
    }
  }

  public Graph<V, E> getGraph() {
    return graph;
  }

  private boolean isConnectedComponentExhausted() {
    for (;;) {
      if (stack.isEmpty()) {
        return true;
      }
      if (stack.getLast() != SENTINEL) {
        return false;
      }

      stack.removeLast();

      @SuppressWarnings("unchecked")
      V v = (V) stack.removeLast();
      seen.put(v, VisitColor.BLACK);
    }
  }

  public boolean hasNext() {
    if (startVertex != null) {
      seen.put(startVertex, VisitColor.WHITE);
      stack.addLast(startVertex);
      startVertex = null;
    }

    if (isConnectedComponentExhausted()) {
      if (isCrossComponentTraversal()) {
        while (vertexIterator.hasNext()) {
          V v = vertexIterator.next();

          if (!seen.containsKey(v)) {
            seen.put(v, VisitColor.WHITE);
            stack.addLast(v);
            return true;
          }
        }

        return false;
      } else {
        return false;
      }
    } else {
      return true;
    }
  }

  @SuppressWarnings("unchecked")
  private V provideNextVertex() {
    V v;
    for (;;) {
      Object o = stack.removeLast();
      if (o == SENTINEL) {
        v = (V) stack.removeLast();
        seen.put(v, VisitColor.BLACK);
      } else {
        v = (V) o;
        break;
      }
    }

    stack.addLast(v);
    stack.addLast(SENTINEL);
    seen.put(v, VisitColor.GRAY);
    return v;
  }

  public V next() {
    if (startVertex != null) {
      seen.put(startVertex, VisitColor.WHITE);
      stack.addLast(startVertex);
      startVertex = null;
    }

    if (hasNext()) {
      V nextVertex = provideNextVertex();

      List<E> edges = Lists.newArrayList(graph.outgoingEdgesOf(nextVertex));
      Collections.reverse(edges);
      for (E edge : edges) {
        V vertex = Graphs.getOppositeVertex(graph, edge, nextVertex);
        if (seen.containsKey(vertex)) {
          VisitColor color = seen.get(vertex);
          if (color == VisitColor.WHITE) {
            stack.removeLastOccurrence(vertex);
            stack.addLast(vertex);
          }
        } else {
          seen.put(vertex, VisitColor.WHITE);
          stack.addLast(vertex);
        }
      }

      return nextVertex;
    } else {
      throw new NoSuchElementException();
    }
  }
}
