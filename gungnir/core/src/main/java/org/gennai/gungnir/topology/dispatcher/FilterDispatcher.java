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

package org.gennai.gungnir.topology.dispatcher;

import static org.gennai.gungnir.GungnirConst.*;

import java.util.List;

import org.gennai.gungnir.topology.operator.Operator;
import org.gennai.gungnir.tuple.TupleValues;

import com.google.common.collect.Lists;

public final class FilterDispatcher extends BaseDispatcher implements Cloneable {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private List<DispatchFilter> dispatchFilters;
  private Dispatcher dispatcher;
  private int index = 0;

  private FilterDispatcher(List<DispatchFilter> dispatchFilters, Dispatcher dispatcher) {
    this.dispatchFilters = dispatchFilters;
    this.dispatcher = dispatcher;
  }

  private FilterDispatcher(FilterDispatcher c) {
    super(c);

    this.dispatchFilters = c.dispatchFilters;
    this.dispatcher = c.dispatcher;

    doPrepare(c.getConfig(), c.getContext());
  }

  public interface FilterDeclarer {

    DispatcherDeclarer filter(DispatchFilter dispatchFilter);
  }

  public interface DispatcherDeclarer extends FilterDeclarer {

    SourceDeclarer dispatcher(Dispatcher dispatcher);
  }

  public interface SourceDeclarer {

    BuildDeclarer source(Operator source);
  }

  public interface BuildDeclarer {

    FilterDispatcher build();
  }

  public static final class Builder implements DispatcherDeclarer, SourceDeclarer,
      BuildDeclarer {

    private List<DispatchFilter> dispatchFilters;
    private Dispatcher dispatcher;
    private Operator source;

    private Builder() {
    }

    @Override
    public DispatcherDeclarer filter(DispatchFilter dispatchFilter) {
      if (this.dispatchFilters == null) {
        this.dispatchFilters = Lists.newArrayList();
      }
      this.dispatchFilters.add(dispatchFilter);
      return this;
    }

    @Override
    public SourceDeclarer dispatcher(Dispatcher dispatcher) {
      this.dispatcher = dispatcher;
      return this;
    }

    @Override
    public Builder source(Operator source) {
      this.source = source;
      return this;
    }

    @Override
    public FilterDispatcher build() {
      return new FilterDispatcher(dispatchFilters, dispatcher).setSource(source);
    }
  }

  public static FilterDeclarer builder() {
    return new Builder();
  }

  public void setDispatcher(Dispatcher dispatcher) {
    this.dispatcher = dispatcher;
  }

  @Override
  protected void prepare() {
  }

  @Override
  public void dispatch(TupleValues tupleValues) {
    if (dispatchFilters != null) {
      if (index < dispatchFilters.size()) {
        dispatchFilters.get(index++).doFilter(tupleValues, this);
      } else {
        index = 0;
        if (!dispatcher.isPrepared()) {
          dispatcher.doPrepare(getConfig(), getContext());
        }
        dispatcher.dispatch(tupleValues);
      }
    } else {
      if (!dispatcher.isPrepared()) {
        dispatcher.doPrepare(getConfig(), getContext());
      }
      dispatcher.dispatch(tupleValues);
    }
  }

  @Override
  public void cleanup() {
    if (dispatcher.isPrepared() && !dispatcher.isCleanedup()) {
      dispatcher.doCleanup();
    }
  }

  @Override
  public FilterDispatcher clone() {
    return new FilterDispatcher(this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (DispatchFilter dispatchFilter : dispatchFilters) {
      sb.append(dispatchFilter);
    }
    sb.append(dispatcher);
    return sb.toString();
  }
}
