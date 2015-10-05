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

package org.gennai.gungnir.tuple.persistent;

public class TrackingData {

  private String tid;
  private Integer tno;
  private String tupleName;
  private Object content;

  public TrackingData(String tupleName, Object content) {
    this.tupleName = tupleName;
    this.content = content;
  }

  public TrackingData(String tupleName, Object content, String tid) {
    this.tupleName = tupleName;
    this.content = content;
    this.tid = tid;
  }

  public void setTid(String tid) {
    this.tid = tid;
  }

  public String getTid() {
    return tid;
  }

  public void setTno(Integer tno) {
    this.tno = tno;
  }

  public Integer getTno() {
    return tno;
  }

  public String getTupleName() {
    return tupleName;
  }

  public Object getContent() {
    return content;
  }

  @Override
  public String toString() {
    return "{tid=" + tid + ", tno=" + tno + ", tupleName=" + tupleName + ", content=" + content
        + "}";
  }
}
