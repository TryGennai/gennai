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

import org.gennai.gungnir.GungnirConfig;
import org.gennai.gungnir.log.DebugLogger;
import org.gennai.gungnir.topology.GungnirContext;
import org.gennai.gungnir.topology.operator.Operator;

public abstract class BaseDispatcher implements Dispatcher {

  private static final long serialVersionUID = SERIAL_VERSION_UID;

  private Operator source;

  private GungnirConfig config;
  private GungnirContext context;
  private DebugLogger debugLogger;
  private transient boolean prepared = false;
  private transient boolean cleanedup = false;

  protected BaseDispatcher() {
  }

  protected BaseDispatcher(BaseDispatcher c) {
    this.source = c.source;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Dispatcher> T setSource(Operator source) {
    this.source = source;
    return (T) this;
  }

  protected GungnirConfig getConfig() {
    return config;
  }

  protected GungnirContext getContext() {
    return context;
  }

  protected DebugLogger getDebugLogger() {
    return debugLogger;
  }

  protected Operator getSource() {
    return source;
  }

  @Override
  public boolean isPrepared() {
    return prepared;
  }

  protected abstract void prepare();

  @Override
  public void doPrepare(GungnirConfig config, GungnirContext context) {
    this.config = config;
    this.context = context;
    debugLogger = context.getDebugLogger(config);

    prepare();
    this.prepared = true;
  }

  @Override
  public boolean isCleanedup() {
    return cleanedup;
  }

  protected void cleanup() {
  }

  @Override
  public void doCleanup() {
    if (!cleanedup) {
      cleanup();
      cleanedup = true;
    }
  }
}
