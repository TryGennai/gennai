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

package org.gennai.gungnir.utils;

import java.nio.ByteBuffer;

public class IntArrayUtils {

  private int size;
  private ByteBuffer buff;

  public IntArrayUtils(int size) {
    this.size = size * 4;
    buff = ByteBuffer.allocate(this.size);
  }

  public byte[] create(int... values) {
    buff.rewind();
    for (int v : values) {
      buff.putInt(v);
    }
    buff.rewind();
    byte[] bytes = new byte[size];
    buff.get(bytes);
    return bytes;
  }

  public void set(byte[] bytes, int index, int value) {
    buff.rewind();
    buff.put(bytes);
    buff.position(index * 4);
    buff.putInt(value);
    buff.rewind();
    buff.get(bytes);
  }

  public int incr(byte[] bytes, int index) {
    buff.rewind();
    buff.put(bytes);
    buff.position(index * 4);
    int value = buff.getInt() + 1;
    buff.position(index * 4);
    buff.putInt(value);
    buff.rewind();
    buff.get(bytes);
    return value;
  }

  public int decr(byte[] bytes, int index) {
    buff.rewind();
    buff.put(bytes);
    buff.position(index * 4);
    int value = buff.getInt();
    if (value > 0) {
      value--;
    }
    buff.position(index * 4);
    buff.putInt(value);
    buff.rewind();
    buff.get(bytes);
    return value;
  }

  public int get(byte[] bytes, int index) {
    buff.rewind();
    buff.put(bytes);
    buff.position(index * 4);
    return buff.getInt();
  }

  public int[] get(byte[] bytes) {
    int[] values = new int[bytes.length / 4];
    buff.rewind();
    buff.put(bytes);
    buff.rewind();
    for (int i = 0; i < values.length; i++) {
      values[i] = buff.getInt();
    }
    return values;
  }

  public boolean forwardMatch(byte[] bytes1, byte[] bytes2, int length) {
    int len = length * 4;
    for (int i = 0; i < len; i++) {
      if (bytes1[i] != bytes2[i]) {
        return false;
      }
    }
    return true;
  }

  public String toString(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    int len = bytes.length / 4;
    for (int i = 0; i < len; i++) {
      if (i > 0) {
        sb.append(':');
      }
      sb.append(get(bytes, i));
    }
    return sb.toString();
  }
}
