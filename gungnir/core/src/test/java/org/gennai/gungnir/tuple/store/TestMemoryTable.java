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

package org.gennai.gungnir.tuple.store;

import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;

import java.util.Iterator;
import java.util.List;

import mockit.Mock;
import mockit.MockUp;
import mockit.integration.junit4.JMockit;

import org.gennai.gungnir.tuple.store.MemoryTable.Entry;
import org.gennai.gungnir.tuple.store.MemoryTable.SeekIterator;
import org.gennai.gungnir.utils.GungnirUtils;
import org.gennai.gungnir.utils.KryoSerializer;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.google.common.collect.Lists;
import com.google.common.primitives.UnsignedBytes;

@RunWith(JMockit.class)
public class TestMemoryTable {

  private MemoryTable memoryTable = new MemoryTable();

  @Test
  public void testIterator() throws Exception {
    memoryTable.put("aaa", 3, new byte[] {1, 1, 1});
    memoryTable.put("aaa", 1, new byte[] {2, 2, 2});
    memoryTable.put("bbb", 1, new byte[] {3, 3, 3});
    memoryTable.put("aaa", 1, new byte[] {4, 4, 4});
    memoryTable.put("aaa", 2, new byte[] {5, 5, 5});
    memoryTable.put("ccc", 2, new byte[] {6, 6, 6});
    memoryTable.put("aaa", 3, new byte[] {7, 7, 7});
    memoryTable.put("bbb", 3, new byte[] {8, 8, 8});
    memoryTable.put("aaa", 5, new byte[] {9, 9, 9});
    memoryTable.put("ccc", 1, new byte[] {10, 10, 10});

    assertThat(memoryTable.getStartTime(), is(1));
    assertThat(memoryTable.size(), is(10));
    assertThat(memoryTable.byteSize(), is(30));
    assertThat(memoryTable.size("aaa"), is(6));
    assertThat(memoryTable.isEmpty(), is(false));

    byte[][] expected = {
        {2, 2, 2},
        {4, 4, 4},
        {5, 5, 5},
        {1, 1, 1},
        {7, 7, 7},
        {9, 9, 9}
    };
    int i = 0;
    for (Iterator<Entry> it = memoryTable.iterator("aaa"); it.hasNext();) {
      Entry entry = it.next();
      assertThat(entry.getValue(), is(expected[i]));
      i++;
    }

    assertThat(i, is(expected.length));
    assertThat(memoryTable.size(), is(10));
    assertThat(memoryTable.byteSize(), is(30));
    assertThat(memoryTable.size("aaa"), is(6));
    assertThat(memoryTable.isEmpty("aaa"), is(false));
    assertThat(memoryTable.size("bbb"), is(2));
    assertThat(memoryTable.isEmpty("bbb"), is(false));

    expected = new byte[][] {
        {2, 2, 2},
        {4, 4, 4},
        {5, 5, 5}
    };
    i = 0;
    for (Iterator<Entry> it = memoryTable.iterator("aaa"); it.hasNext();) {
      Entry entry = it.next();
      if (entry.getTimeKey() < 3) {
        assertThat(entry.getValue(), is(expected[i]));
        i++;
        it.remove();
      } else {
        break;
      }
    }

    assertThat(i, is(expected.length));
    assertThat(memoryTable.size(), is(7));
    assertThat(memoryTable.byteSize(), is(21));
    assertThat(memoryTable.size("aaa"), is(3));
    assertThat(memoryTable.isEmpty("aaa"), is(false));
    assertThat(memoryTable.size("bbb"), is(2));
    assertThat(memoryTable.isEmpty("bbb"), is(false));

    expected = new byte[][] {
        {1, 1, 1},
        {7, 7, 7},
        {9, 9, 9}
    };
    i = 0;
    for (Iterator<Entry> it = memoryTable.iterator("aaa"); it.hasNext();) {
      Entry entry = it.next();
      if (entry.getTimeKey() < 6) {
        assertThat(entry.getValue(), is(expected[i]));
        i++;
        it.remove();
      } else {
        break;
      }
    }

    assertThat(i, is(expected.length));
    assertThat(memoryTable.size(), is(4));
    assertThat(memoryTable.byteSize(), is(12));
    assertThat(memoryTable.size("aaa"), is(0));
    assertThat(memoryTable.isEmpty("aaa"), is(true));
    assertThat(memoryTable.size("bbb"), is(2));
    assertThat(memoryTable.isEmpty("bbb"), is(false));

    expected = new byte[][] {
        {3, 3, 3}
    };
    i = 0;
    for (Iterator<Entry> it = memoryTable.iterator("bbb"); it.hasNext();) {
      Entry entry = it.next();
      if (entry.getTimeKey() < 2) {
        assertThat(entry.getValue(), is(expected[i]));
        i++;
        it.remove();
      } else {
        break;
      }
    }

    assertThat(i, is(expected.length));
    assertThat(memoryTable.size(), is(3));
    assertThat(memoryTable.byteSize(), is(9));
    assertThat(memoryTable.size("bbb"), is(1));
    assertThat(memoryTable.isEmpty("bbb"), is(false));

    memoryTable.clear();

    assertThat(memoryTable.size(), is(0));
    assertThat(memoryTable.byteSize(), is(0));
    assertThat(memoryTable.isEmpty("ccc"), is(true));
  }

  @Test
  public void testIterator2() throws Exception {
    memoryTable.put(Lists.newArrayList("aaa", "bbb"), 3, new byte[] {1, 1});
    memoryTable.put(Lists.newArrayList("aaa", "bbb"), 1, new byte[] {2, 2});
    memoryTable.put(Lists.newArrayList("aaa", "bbb", "ccc"), 1, new byte[] {3, 3});
    memoryTable.put(Lists.newArrayList("aaa", "bbb"), 1, new byte[] {4, 4});
    memoryTable.put(Lists.newArrayList("aaa", "bbb"), 2, new byte[] {5, 5});
    memoryTable.put(Lists.newArrayList("aaa", "bb2"), 2, new byte[] {6, 6});
    memoryTable.put(Lists.newArrayList("aaa", "bbb"), 3, new byte[] {7, 7});
    memoryTable.put(Lists.newArrayList("aaa", "bbb", "ccc"), 3, new byte[] {8, 8});
    memoryTable.put(Lists.newArrayList("aaa", "bbb"), 5, new byte[] {9, 9});

    assertThat(memoryTable.getStartTime(), is(1));
    assertThat(memoryTable.size(), is(9));
    assertThat(memoryTable.byteSize(), is(18));
    assertThat(memoryTable.size(Lists.newArrayList("aaa", "bbb")), is(6));
    assertThat(memoryTable.isEmpty(), is(false));

    byte[][] expected = {
        {2, 2},
        {4, 4},
        {5, 5},
        {1, 1},
        {7, 7},
        {9, 9}
    };
    int i = 0;
    for (Iterator<Entry> it =
        memoryTable.iterator(Lists.newArrayList("aaa", "bbb")); it.hasNext();) {
      Entry entry = it.next();
      assertThat(entry.getValue(), is(expected[i]));
      i++;
    }

    assertThat(i, is(expected.length));
    assertThat(memoryTable.size(), is(9));
    assertThat(memoryTable.byteSize(), is(18));
    assertThat(memoryTable.size(Lists.newArrayList("aaa", "bbb")), is(6));
    assertThat(memoryTable.isEmpty(Lists.newArrayList("aaa", "bbb")), is(false));

    expected = new byte[][] {
        {2, 2},
        {4, 4},
        {5, 5}
    };
    i = 0;
    for (Iterator<Entry> it =
        memoryTable.iterator(Lists.newArrayList("aaa", "bbb")); it.hasNext();) {
      Entry entry = it.next();
      if (entry.getTimeKey() < 3) {
        assertThat(entry.getValue(), is(expected[i]));
        i++;
        it.remove();
      } else {
        break;
      }
    }

    assertThat(i, is(expected.length));
    assertThat(memoryTable.size(), is(6));
    assertThat(memoryTable.byteSize(), is(12));
    assertThat(memoryTable.size(Lists.newArrayList("aaa", "bbb")), is(3));
    assertThat(memoryTable.isEmpty(Lists.newArrayList("aaa", "bbb")), is(false));

    expected = new byte[][] {
        {1, 1},
        {7, 7},
        {9, 9}
    };
    i = 0;
    for (Iterator<Entry> it =
        memoryTable.iterator(Lists.newArrayList("aaa", "bbb")); it.hasNext();) {
      Entry entry = it.next();
      if (entry.getTimeKey() < 6) {
        assertThat(entry.getValue(), is(expected[i]));
        i++;
        it.remove();
      } else {
        break;
      }
    }

    assertThat(i, is(expected.length));
    assertThat(memoryTable.size(), is(3));
    assertThat(memoryTable.byteSize(), is(6));
    assertThat(memoryTable.size(Lists.newArrayList("aaa", "bbb")), is(0));
    assertThat(memoryTable.isEmpty(Lists.newArrayList("aaa", "bbb")), is(true));

    memoryTable.clear();

    assertThat(memoryTable.size(), is(0));
    assertThat(memoryTable.byteSize(), is(0));
    assertThat(memoryTable.isEmpty(Lists.newArrayList("aaa", "bbb", "ccc")), is(true));
  }

  @Test
  public void testIterator3() throws Exception {
    memoryTable.put(3, new byte[] {1});
    memoryTable.put(1, new byte[] {2});
    memoryTable.put(1, new byte[] {3});
    memoryTable.put(3, new byte[] {4});
    memoryTable.put(2, new byte[] {5});
    memoryTable.put(2, new byte[] {6});

    assertThat(memoryTable.size(), is(6));
    assertThat(memoryTable.getStartTime(), is(1));
    assertThat(memoryTable.byteSize(), is(6));

    byte[][] expected = {
        {2},
        {3},
        {5},
        {6}
    };
    int i = 0;
    for (Iterator<Entry> it = memoryTable.iterator(); it.hasNext();) {
      Entry entry = it.next();
      if (entry.getTimeKey() < 3) {
        assertThat(entry.getValue(), is(expected[i]));
        i++;
      }
    }

    assertThat(i, is(expected.length));
    assertThat(memoryTable.size(), is(6));
    assertThat(memoryTable.byteSize(), is(6));

    expected = new byte[][] {
        {2},
        {3},
        {5},
        {6}
    };
    i = 0;
    for (Iterator<Entry> it = memoryTable.iterator(); it.hasNext();) {
      Entry entry = it.next();
      if (entry.getTimeKey() < 3) {
        assertThat(entry.getValue(), is(expected[i]));
        i++;
        it.remove();
      }
    }

    assertThat(i, is(expected.length));
    assertThat(memoryTable.size(), is(2));
    assertThat(memoryTable.byteSize(), is(2));

    expected = new byte[][] {
        {1},
        {4}
    };
    i = 0;
    for (Iterator<Entry> it = memoryTable.iterator(); it.hasNext();) {
      Entry entry = it.next();
      if (entry.getTimeKey() < 5) {
        assertThat(entry.getValue(), is(expected[i]));
        i++;
        it.remove();
      }
    }

    assertThat(i, is(expected.length));
    assertThat(memoryTable.size(), is(0));
    assertThat(memoryTable.byteSize(), is(0));
  }

  @Test
  public void testIterator4() throws Exception {
    memoryTable.incr("aaa", 2);
    memoryTable.incr("aaa", 1);
    memoryTable.incr("bbb", 2);
    memoryTable.incr("aaa", 2);
    memoryTable.incr("aaa", 1);
    memoryTable.incr("aaa", 5);
    memoryTable.incr("aaa", 2);
    memoryTable.incr("bbb", 5);
    memoryTable.incr("aaa", 1);
    memoryTable.incr("aaa", 2);
    memoryTable.incr("bbb", 5);
    memoryTable.incr("aaa", 5);
    memoryTable.incr("bbb", 5);
    memoryTable.incr("aaa", 1);
    memoryTable.incr("aaa", 2);
    memoryTable.incr("aaa", 5);
    memoryTable.incr("ccc", 4);
    memoryTable.incr("aaa", 4);

    assertThat(memoryTable.getStartTime(), is(1));
    assertThat(memoryTable.size(), is(18));
    assertThat(memoryTable.byteSize(), is(28));
    assertThat(memoryTable.size("aaa"), is(13));
    assertThat(memoryTable.isEmpty(), is(false));

    byte[][] expected = {
        {0, 0, 0, 4},
        {0, 0, 0, 5}
    };
    int i = 0;
    for (Iterator<Entry> it = memoryTable.iterator("aaa"); it.hasNext();) {
      Entry entry = it.next();
      if (entry.getTimeKey() < 4) {
        assertThat(entry.getValue(), is(expected[i]));
        i++;
      }
    }

    assertThat(i, is(expected.length));
    assertThat(memoryTable.size(), is(18));
    assertThat(memoryTable.byteSize(), is(28));
    assertThat(memoryTable.size("aaa"), is(13));
    assertThat(memoryTable.isEmpty(), is(false));

    expected = new byte[][] {
        {0, 0, 0, 4},
        {0, 0, 0, 5}
    };
    i = 0;
    for (Iterator<Entry> it = memoryTable.iterator("aaa"); it.hasNext();) {
      Entry entry = it.next();
      if (entry.getTimeKey() < 4) {
        assertThat(entry.getValue(), is(expected[i]));
        i++;
        it.remove();
      }
    }

    assertThat(i, is(expected.length));
    assertThat(memoryTable.size(), is(9));
    assertThat(memoryTable.byteSize(), is(20));
    assertThat(memoryTable.size("aaa"), is(4));
    assertThat(memoryTable.isEmpty("aaa"), is(false));

    memoryTable.clear();

    memoryTable.incr(3);
    memoryTable.incr(1);
    memoryTable.incr(2);
    memoryTable.incr(1);
    memoryTable.incr(2);
    memoryTable.incr(1);

    assertThat(memoryTable.getStartTime(), is(1));
    assertThat(memoryTable.size(), is(6));
    assertThat(memoryTable.byteSize(), is(12));

    expected = new byte[][] {
        {0, 0, 0, 3},
        {0, 0, 0, 2}
    };
    i = 0;
    for (Iterator<Entry> it = memoryTable.iterator(); it.hasNext();) {
      Entry entry = it.next();
      if (entry.getTimeKey() < 3) {
        assertThat(entry.getValue(), is(expected[i]));
        i++;
        it.remove();
      }
    }

    assertThat(i, is(expected.length));
    assertThat(memoryTable.size(), is(1));
    assertThat(memoryTable.byteSize(), is(4));

    expected = new byte[][] {
        {0, 0, 0, 1}
    };
    i = 0;
    for (Iterator<Entry> it = memoryTable.iterator(); it.hasNext();) {
      Entry entry = it.next();
      if (entry.getTimeKey() < 4) {
        assertThat(entry.getValue(), is(expected[i]));
        i++;
        it.remove();
      }
    }

    assertThat(i, is(expected.length));
    assertThat(memoryTable.size(), is(0));
    assertThat(memoryTable.byteSize(), is(0));
  }

  @Test
  public void testIterator5() throws Exception {
    memoryTable.put("aaa", 3, new byte[] {1, 1, 1});
    memoryTable.put("aaa", 1, new byte[] {2, 2, 2});
    memoryTable.put("bbb", 1, new byte[] {3, 3, 3});
    memoryTable.put("aaa", 1, new byte[] {4, 4, 4});
    memoryTable.put("aaa", 2, new byte[] {5, 5, 5});
    memoryTable.put("ccc", 2, new byte[] {6, 6, 6});
    memoryTable.put("aaa", 3, new byte[] {7, 7, 7});
    memoryTable.put("bbb", 3, new byte[] {8, 8, 8});
    memoryTable.put("aaa", 5, new byte[] {9, 9, 9});
    memoryTable.put("ccc", 1, new byte[] {10, 10, 10});

    byte[][] expected = {
        {2, 2, 2},
        {4, 4, 4},
        {5, 5, 5},
        {1, 1, 1},
        {7, 7, 7},
        {9, 9, 9},
        {3, 3, 3},
        {8, 8, 8},
        {10, 10, 10},
        {6, 6, 6}
    };
    int i = 0;
    for (Iterator<Entry> it = memoryTable.iterator(); it.hasNext();) {
      Entry entry = it.next();
      assertThat(entry.getValue(), is(expected[i]));
      i++;
    }

    assertThat(i, is(expected.length));

    i = 0;
    for (Iterator<Entry> it = memoryTable.iterator(); it.hasNext();) {
      Entry entry = it.next();
      assertThat(entry.getValue(), is(expected[i]));
      i++;
      it.remove();
    }

    assertThat(i, is(expected.length));
    assertThat(memoryTable.size(), is(0));
    assertThat(memoryTable.byteSize(), is(0));

    memoryTable.incr("aaa", 2);
    memoryTable.incr("aaa", 1);
    memoryTable.incr("bbb", 2);
    memoryTable.incr("aaa", 2);
    memoryTable.incr("aaa", 1);
    memoryTable.incr("aaa", 5);
    memoryTable.incr("aaa", 2);
    memoryTable.incr("bbb", 5);
    memoryTable.incr("aaa", 1);
    memoryTable.incr("aaa", 2);
    memoryTable.incr("bbb", 5);
    memoryTable.incr("aaa", 5);
    memoryTable.incr("bbb", 5);
    memoryTable.incr("aaa", 1);
    memoryTable.incr("aaa", 2);
    memoryTable.incr("aaa", 5);
    memoryTable.incr("ccc", 4);
    memoryTable.incr("aaa", 4);

    expected = new byte[][] {
        {0, 0, 0, 4},
        {0, 0, 0, 5},
        {0, 0, 0, 1},
        {0, 0, 0, 3},
        {0, 0, 0, 1},
        {0, 0, 0, 3},
        {0, 0, 0, 1}
    };
    i = 0;
    for (Iterator<Entry> it = memoryTable.iterator(); it.hasNext();) {
      Entry entry = it.next();
      assertThat(entry.getValue(), is(expected[i]));
      i++;
    }

    assertThat(i, is(expected.length));

    i = 0;
    for (Iterator<Entry> it = memoryTable.iterator(); it.hasNext();) {
      Entry entry = it.next();
      assertThat(entry.getValue(), is(expected[i]));
      i++;
      it.remove();
    }

    assertThat(i, is(expected.length));
    assertThat(memoryTable.size(), is(0));
    assertThat(memoryTable.byteSize(), is(0));
  }

  @Test
  public void testRemoveFirst() throws Exception {
    memoryTable.put("aaa", 3, new byte[] {1, 1, 1});
    memoryTable.put("aaa", 1, new byte[] {2, 2, 2});
    memoryTable.put("bbb", 1, new byte[] {3, 3, 3});
    memoryTable.put("aaa", 1, new byte[] {4, 4, 4});
    memoryTable.put("aaa", 2, new byte[] {5, 5, 5});
    memoryTable.put("ccc", 2, new byte[] {6, 6, 6});
    memoryTable.put("aaa", 3, new byte[] {7, 7, 7});
    memoryTable.put("bbb", 3, new byte[] {8, 8, 8});
    memoryTable.put("aaa", 5, new byte[] {9, 9, 9});
    memoryTable.put("ccc", 1, new byte[] {10, 10, 10});

    assertThat(memoryTable.removeFirst("aaa").getValue(), is(new byte[] {2, 2, 2}));
    assertThat(memoryTable.removeFirst("aaa").getValue(), is(new byte[] {4, 4, 4}));
    assertThat(memoryTable.removeFirst("aaa").getValue(), is(new byte[] {5, 5, 5}));

    assertThat(memoryTable.size(), is(7));
    assertThat(memoryTable.byteSize(), is(21));
    assertThat(memoryTable.size("aaa"), is(3));
    assertThat(memoryTable.isEmpty("aaa"), is(false));

    assertThat(memoryTable.removeFirst("aaa").getValue(), is(new byte[] {1, 1, 1}));
    assertThat(memoryTable.removeFirst("aaa").getValue(), is(new byte[] {7, 7, 7}));
    assertThat(memoryTable.removeFirst("aaa").getValue(), is(new byte[] {9, 9, 9}));

    assertThat(memoryTable.size(), is(4));
    assertThat(memoryTable.byteSize(), is(12));
    assertThat(memoryTable.size("aaa"), is(0));
    assertThat(memoryTable.isEmpty("aaa"), is(true));

    assertThat(memoryTable.removeFirst("aaa"), nullValue());

    assertThat(memoryTable.removeFirst("bbb").getValue(), is(new byte[] {3, 3, 3}));
    assertThat(memoryTable.removeFirst("bbb").getValue(), is(new byte[] {8, 8, 8}));
    assertThat(memoryTable.removeFirst("ccc").getValue(), is(new byte[] {10, 10, 10}));
    assertThat(memoryTable.removeFirst("ccc").getValue(), is(new byte[] {6, 6, 6}));

    assertThat(memoryTable.size(), is(0));
    assertThat(memoryTable.byteSize(), is(0));
  }

  @Test
  public void testRemoveFirst2() throws Exception {
    memoryTable.put(Lists.newArrayList("aaa", "bbb"), 3, new byte[] {1, 1});
    memoryTable.put(Lists.newArrayList("aaa", "bbb"), 1, new byte[] {2, 2});
    memoryTable.put(Lists.newArrayList("aaa", "bbb", "ccc"), 1, new byte[] {3, 3});
    memoryTable.put(Lists.newArrayList("aaa", "bbb"), 1, new byte[] {4, 4});
    memoryTable.put(Lists.newArrayList("aaa", "bbb"), 2, new byte[] {5, 5});
    memoryTable.put(Lists.newArrayList("aaa", "bb2"), 2, new byte[] {6, 6});
    memoryTable.put(Lists.newArrayList("aaa", "bbb"), 3, new byte[] {7, 7});
    memoryTable.put(Lists.newArrayList("aaa", "bbb", "ccc"), 3, new byte[] {8, 8});
    memoryTable.put(Lists.newArrayList("aaa", "bbb"), 5, new byte[] {9, 9});

    assertThat(memoryTable.removeFirst(Lists.newArrayList("aaa", "bbb")).getValue(),
        is(new byte[] {2, 2}));
    assertThat(memoryTable.removeFirst(Lists.newArrayList("aaa", "bbb")).getValue(),
        is(new byte[] {4, 4}));
    assertThat(memoryTable.removeFirst(Lists.newArrayList("aaa", "bbb")).getValue(),
        is(new byte[] {5, 5}));

    assertThat(memoryTable.size(), is(6));
    assertThat(memoryTable.byteSize(), is(12));
    assertThat(memoryTable.size(Lists.newArrayList("aaa", "bbb")), is(3));
    assertThat(memoryTable.isEmpty(Lists.newArrayList("aaa", "bbb")), is(false));

    memoryTable.clear();
  }

  @Test
  public void testRemoveFirst3() throws Exception {
    memoryTable.put(3, new byte[] {1});
    memoryTable.put(1, new byte[] {2});
    memoryTable.put(1, new byte[] {3});
    memoryTable.put(3, new byte[] {4});
    memoryTable.put(2, new byte[] {5});
    memoryTable.put(2, new byte[] {6});

    assertThat(memoryTable.removeFirst().getValue(), is(new byte[] {2}));
    assertThat(memoryTable.removeFirst().getValue(), is(new byte[] {3}));
    assertThat(memoryTable.removeFirst().getValue(), is(new byte[] {5}));

    assertThat(memoryTable.size(), is(3));
    assertThat(memoryTable.byteSize(), is(3));

    assertThat(memoryTable.removeFirst().getValue(), is(new byte[] {6}));
    assertThat(memoryTable.removeFirst().getValue(), is(new byte[] {1}));
    assertThat(memoryTable.removeFirst().getValue(), is(new byte[] {4}));
    assertThat(memoryTable.removeFirst(), nullValue());

    assertThat(memoryTable.size(), is(0));
    assertThat(memoryTable.byteSize(), is(0));

    memoryTable.clear();
  }

  private SeekIterator seekIterator;
  private Entry seekEntry;

  private void seek(int expireSec, int seekSize, Object[][] expected) {
    if (seekEntry == null) {
      seekIterator.seekToFirst();
    } else {
      seekIterator.seek(seekEntry.getHashKey(), seekEntry.getTimeKey(), seekEntry.getSeqNo(),
          false);
      if (!seekIterator.isValid()) {
        seekIterator.seekToFirst();
      }
    }

    int j = 0;
    for (int i = 0; seekIterator.isValid() && i < seekSize; i++) {
      Entry entry = seekIterator.entry();
      seekEntry = entry;
      if (entry.getTimeKey() < expireSec) {
        assertThat(entry.getHashKey(), is(expected[j][0]));
        assertThat(entry.getTimeKey(), is(expected[j][1]));
        assertThat(entry.getValue(), is(expected[j][2]));
        j++;
        seekIterator.remove();
        seekIterator.next();
      } else {
        entry.setTimeKey(Integer.MAX_VALUE);
        entry.setSeqNo(Integer.MAX_VALUE);
        seekIterator.seek(entry.getHashKey(), entry.getTimeKey(), entry.getSeqNo(), false);
      }
    }

    if (expected != null) {
      assertThat(j, is(expected.length));
    } else {
      assertThat(j, is(0));
    }
  }

  @Test
  public void testSeekIterator() throws Exception {
    memoryTable.put("aaa", 3, new byte[] {1, 1, 1});
    memoryTable.put("aaa", 1, new byte[] {2, 2, 2});
    memoryTable.put("bbb", 1, new byte[] {3, 3, 3});
    memoryTable.put("aaa", 1, new byte[] {4, 4, 4});
    memoryTable.put("aaa", 2, new byte[] {5, 5, 5});
    memoryTable.put("ccc", 2, new byte[] {6, 6, 6});
    memoryTable.put("aaa", 3, new byte[] {7, 7, 7});
    memoryTable.put("bbb", 3, new byte[] {8, 8, 8});
    memoryTable.put("aaa", 5, new byte[] {9, 9, 9});
    memoryTable.put("ccc", 1, new byte[] {10, 10, 10});

    seekIterator = memoryTable.seekIterator();
    seekEntry = null;
    seek(1, 8, null);

    Object[][] expected = {
        {"aaa", 1, new byte[] {2, 2, 2}},
        {"aaa", 1, new byte[] {4, 4, 4}},
        {"bbb", 1, new byte[] {3, 3, 3}},
        {"ccc", 1, new byte[] {10, 10, 10}}
    };
    seek(2, 8, expected);

    seek(2, 8, null);

    memoryTable.put("aaa", 4, new byte[] {11, 11, 11});
    memoryTable.put("ddd", 3, new byte[] {12, 12, 12});
    memoryTable.put("bbb", 3, new byte[] {13, 13, 13});
    memoryTable.put("bbb", 3, new byte[] {14, 14, 14});

    expected = new Object[][] {
        {"ddd", 3, new byte[] {12, 12, 12}}
    };
    seek(4, 8, expected);

    expected = new Object[][] {
        {"aaa", 2, new byte[] {5, 5, 5}},
        {"aaa", 3, new byte[] {1, 1, 1}},
        {"aaa", 3, new byte[] {7, 7, 7}},
        {"bbb", 3, new byte[] {8, 8, 8}}
    };
    seek(4, 5, expected);

    expected = new Object[][] {
        {"bbb", 3, new byte[] {13, 13, 13}},
        {"bbb", 3, new byte[] {14, 14, 14}},
        {"ccc", 2, new byte[] {6, 6, 6}}
    };
    seek(4, 5, expected);

    expected = new Object[][] {
        {"aaa", 4, new byte[] {11, 11, 11}},
        {"aaa", 5, new byte[] {9, 9, 9}}
    };
    seek(6, 5, expected);

    memoryTable.clear();
  }

  @Test
  public void testExpire() throws Exception {
    final int now = GungnirUtils.currentTimeSecs();
    MemoryTable table = new MemoryTable(3, 4);

    table.put("aaa", now + 3, new byte[] {1, 1, 1});
    table.put("aaa", now + 1, new byte[] {2, 2, 2});
    table.put("bbb", now + 1, new byte[] {3, 3, 3});
    table.put("aaa", now + 1, new byte[] {4, 4, 4});
    table.put("aaa", now + 2, new byte[] {5, 5, 5});
    table.put("ccc", now + 2, new byte[] {6, 6, 6});
    table.put("aaa", now + 3, new byte[] {7, 7, 7});
    table.put("bbb", now + 3, new byte[] {8, 8, 8});
    table.put("aaa", now + 5, new byte[] {9, 9, 9});
    table.put("ccc", now + 1, new byte[] {10, 10, 10});

    assertThat(table.size(), is(10));

    new MockUp<GungnirUtils>() {

      @Mock
      public int currentTimeSecs() {
        return now + 6;
      }
    };

    assertThat(table.size("aaa"), is(3));
    assertThat(table.size("bbb"), is(1));
    assertThat(table.size("ccc"), is(0));
    assertThat(table.isEmpty("aaa"), is(false));
    assertThat(table.isEmpty("bbb"), is(false));
    assertThat(table.isEmpty("ccc"), is(true));

    Object[][] expected = {
        {"aaa", now + 6, new byte[] {1, 1, 1}},
        {"aaa", now + 6, new byte[] {7, 7, 7}},
        {"aaa", now + 8, new byte[] {9, 9, 9}},
    };
    int i = 0;
    for (Iterator<Entry> it = table.iterator("aaa"); it.hasNext();) {
      Entry entry = it.next();
      assertThat(entry.getHashKey(), is(expected[i][0]));
      assertThat(entry.getTimeKey(), is(expected[i][1]));
      assertThat(entry.getValue(), is(expected[i][2]));
      i++;
    }

    assertThat(i, is(expected.length));
    assertThat(table.size(), is(10));

    assertThat(table.removeFirst("bbb").getValue(), is(new byte[] {8, 8, 8}));
    assertThat(table.removeFirst("aaa").getValue(), is(new byte[] {1, 1, 1}));
    assertThat(table.removeFirst("aaa").getValue(), is(new byte[] {7, 7, 7}));
    assertThat(table.removeFirst("aaa").getValue(), is(new byte[] {9, 9, 9}));
    assertThat(table.removeFirst("aaa"), nullValue());

    assertThat(table.isEmpty("aaa"), is(true));
    assertThat(table.isEmpty("bbb"), is(true));
    assertThat(table.isEmpty("ccc"), is(true));

    assertThat(table.size(), is(2));

    expected = new Object[][] {
        {"ccc", now + 4, new byte[] {10, 10, 10}},
        {"ccc", now + 5, new byte[] {6, 6, 6}}
    };
    i = 0;
    SeekIterator sit = table.seekIterator();
    for (sit.seekToFirst(); sit.isValid(); sit.next()) {
      Entry entry = sit.entry();
      assertThat(entry.getHashKey(), is(expected[i][0]));
      assertThat(entry.getTimeKey(), is(expected[i][1]));
      assertThat(entry.getValue(), is(expected[i][2]));
      i++;
      sit.remove();
    }

    assertThat(table.size(), is(0));

    new MockUp<GungnirUtils>() {

      @Mock
      public int currentTimeSecs() {
        return now;
      }
    };

    table.put(now + 3, new byte[] {1});
    table.put(now + 1, new byte[] {2});
    table.put(now + 1, new byte[] {3});
    table.put(now + 3, new byte[] {4});
    table.put(now + 2, new byte[] {5});
    table.put(now + 2, new byte[] {6});

    assertThat(table.size(), is(6));

    new MockUp<GungnirUtils>() {

      @Mock
      public int currentTimeSecs() {
        return now + 6;
      }
    };

    assertThat(table.size(), is(2));

    expected = new Object[][] {
        {null, now + 6, new byte[] {1}},
        {null, now + 6, new byte[] {4}}
    };
    i = 0;
    for (Iterator<Entry> it = table.iterator(); it.hasNext();) {
      Entry entry = it.next();
      assertThat(entry.getHashKey(), is(expected[i][0]));
      assertThat(entry.getTimeKey(), is(expected[i][1]));
      assertThat(entry.getValue(), is(expected[i][2]));
      i++;
    }

    assertThat(table.removeFirst().getValue(), is(new byte[] {1}));
    assertThat(table.removeFirst().getValue(), is(new byte[] {4}));
    assertThat(table.removeFirst(), nullValue());

    assertThat(table.size(), is(0));
  }

  // CHECKSTYLE IGNORE MethodLength FOR NEXT 1 LINES
  @Test
  public void testCompactRange() throws Exception {
    final int now = GungnirUtils.currentTimeSecs();
    MemoryTable table = new MemoryTable(3, 4);
    table.compactRange();

    table.put("aaa", now + 3, new byte[] {1, 1, 1});
    table.put("aaa", now + 1, new byte[] {2, 2, 2});
    table.put("bbb", now + 1, new byte[] {3, 3, 3});
    table.put("aaa", now + 1, new byte[] {4, 4, 4});
    table.put("aaa", now + 2, new byte[] {5, 5, 5});
    table.put("ccc", now + 2, new byte[] {6, 6, 6});
    table.put("aaa", now + 3, new byte[] {7, 7, 7});
    table.put("bbb", now + 3, new byte[] {8, 8, 8});
    table.put("aaa", now + 5, new byte[] {9, 9, 9});
    table.put("ccc", now + 1, new byte[] {10, 10, 10});

    assertThat(table.size(), is(10));

    new MockUp<GungnirUtils>() {

      @Mock
      public int currentTimeSecs() {
        return now + 5;
      }
    };

    Object[][] expected = {
        {"aaa", now + 4, new byte[] {2, 2, 2}},
        {"aaa", now + 4, new byte[] {4, 4, 4}},
        {"bbb", now + 4, new byte[] {3, 3, 3}}
    };
    List<Entry> entries = table.compactRange();
    int i = 0;
    for (Entry entry : entries) {
      assertThat(entry.getHashKey(), is(expected[i][0]));
      assertThat(entry.getTimeKey(), is(expected[i][1]));
      assertThat(entry.getValue(), is(expected[i][2]));
      i++;
    }

    assertThat(i, is(expected.length));
    assertThat(table.size(), is(7));
    assertThat(table.size("aaa"), is(4));
    assertThat(table.size("bbb"), is(1));
    assertThat(table.size("ccc"), is(1));
    assertThat(table.isEmpty("aaa"), is(false));
    assertThat(table.isEmpty("bbb"), is(false));
    assertThat(table.isEmpty("ccc"), is(false));

    expected = new Object[][] {
        {"aaa", now + 5, new byte[] {5, 5, 5}},
        {"aaa", now + 6, new byte[] {1, 1, 1}},
        {"aaa", now + 6, new byte[] {7, 7, 7}},
        {"aaa", now + 8, new byte[] {9, 9, 9}}
    };
    i = 0;
    for (Iterator<Entry> it = table.iterator("aaa"); it.hasNext();) {
      Entry entry = it.next();
      assertThat(entry.getHashKey(), is(expected[i][0]));
      assertThat(entry.getTimeKey(), is(expected[i][1]));
      assertThat(entry.getValue(), is(expected[i][2]));
      i++;
    }

    assertThat(i, is(expected.length));
    assertThat(table.size(), is(7));

    expected = new Object[][] {
        {"ccc", now + 4, new byte[] {10, 10, 10}}
    };
    entries = table.compactRange();
    i = 0;
    for (Entry entry : entries) {
      assertThat(entry.getHashKey(), is(expected[i][0]));
      assertThat(entry.getTimeKey(), is(expected[i][1]));
      assertThat(entry.getValue(), is(expected[i][2]));
      i++;
    }

    assertThat(i, is(expected.length));
    assertThat(table.size(), is(6));
    assertThat(table.size("aaa"), is(4));
    assertThat(table.size("bbb"), is(1));
    assertThat(table.size("ccc"), is(1));

    table.put("aaa", now + 4, new byte[] {11, 11, 11});
    table.put("ddd", now + 3, new byte[] {12, 12, 12});
    table.put("bbb", now + 3, new byte[] {13, 13, 13});
    table.put("bbb", now + 3, new byte[] {14, 14, 14});

    new MockUp<GungnirUtils>() {

      @Mock
      public int currentTimeSecs() {
        return now + 8;
      }
    };

    expected = new Object[][] {
        {"ddd", now + 6, new byte[] {12, 12, 12}}
    };
    entries = table.compactRange();
    i = 0;
    for (Entry entry : entries) {
      assertThat(entry.getHashKey(), is(expected[i][0]));
      assertThat(entry.getTimeKey(), is(expected[i][1]));
      assertThat(entry.getValue(), is(expected[i][2]));
      i++;
    }

    assertThat(i, is(expected.length));
    assertThat(table.size(), is(9));
    assertThat(table.size("aaa"), is(1));
    assertThat(table.size("bbb"), is(0));
    assertThat(table.size("ccc"), is(0));
    assertThat(table.size("ddd"), is(0));

    expected = new Object[][] {
        {"aaa", now + 5, new byte[] {5, 5, 5}},
        {"aaa", now + 6, new byte[] {1, 1, 1}},
        {"aaa", now + 6, new byte[] {7, 7, 7}},
        {"aaa", now + 7, new byte[] {11, 11, 11}}
    };
    entries = table.compactRange();
    i = 0;
    for (Entry entry : entries) {
      assertThat(entry.getHashKey(), is(expected[i][0]));
      assertThat(entry.getTimeKey(), is(expected[i][1]));
      assertThat(entry.getValue(), is(expected[i][2]));
      i++;
    }

    assertThat(i, is(expected.length));
    assertThat(table.size(), is(5));

    assertThat(table.removeFirst("aaa").getValue(), is(new byte[] {9, 9, 9}));
    assertThat(table.removeFirst("aaa"), nullValue());

    expected = new Object[][] {
        {"bbb", now + 6, new byte[] {8, 8, 8}},
        {"bbb", now + 6, new byte[] {13, 13, 13}},
        {"bbb", now + 6, new byte[] {14, 14, 14}},
        {"ccc", now + 5, new byte[] {6, 6, 6}}
    };
    i = 0;
    SeekIterator sit = table.seekIterator();
    for (sit.seekToFirst(); sit.isValid(); sit.next()) {
      Entry entry = sit.entry();
      assertThat(entry.getHashKey(), is(expected[i][0]));
      assertThat(entry.getTimeKey(), is(expected[i][1]));
      assertThat(entry.getValue(), is(expected[i][2]));
      i++;
    }

    assertThat(i, is(expected.length));

    table.put("aaa", now + 10, new byte[] {15, 15, 15});
    table.put("bbb", now + 10, new byte[] {16, 16, 16});

    expected = new Object[][] {
        {"bbb", now + 13, new byte[] {16, 16, 16}},
        {"ccc", now + 5, new byte[] {6, 6, 6}},
        {"aaa", now + 13, new byte[] {15, 15, 15}},
    };
    i = 0;
    sit = table.seekIterator();
    for (sit.seekToFirst(); sit.isValid(); sit.next()) {
      Entry entry = sit.entry();
      assertThat(entry.getHashKey(), is(expected[i][0]));
      assertThat(entry.getTimeKey(), is(expected[i][1]));
      assertThat(entry.getValue(), is(expected[i][2]));
      i++;
    }

    assertThat(i, is(expected.length));

    entries = table.compactRange();
    assertThat(entries.isEmpty(), is(true));
    assertThat(table.size(), is(3));

    expected = new Object[][] {
        {"ccc", now + 5, new byte[] {6, 6, 6}}
    };
    entries = table.compactRange();
    i = 0;
    for (Entry entry : entries) {
      assertThat(entry.getHashKey(), is(expected[i][0]));
      assertThat(entry.getTimeKey(), is(expected[i][1]));
      assertThat(entry.getValue(), is(expected[i][2]));
      i++;
    }

    assertThat(i, is(expected.length));

    entries = table.compactRange();
    assertThat(entries.isEmpty(), is(true));
    assertThat(table.size(), is(2));

    assertThat(table.size("aaa"), is(1));
    assertThat(table.size("bbb"), is(1));
    assertThat(table.size("ccc"), is(0));
    assertThat(table.size("ddd"), is(0));

    expected = new Object[][] {
        {"bbb", now + 13, new byte[] {16, 16, 16}},
        {"aaa", now + 13, new byte[] {15, 15, 15}},
    };
    i = 0;
    sit = table.seekIterator();
    for (sit.seekToFirst(); sit.isValid(); sit.next()) {
      assertThat(sit.entry().getHashKey(), is(expected[i][0]));
      assertThat(sit.entry().getTimeKey(), is(expected[i][1]));
      assertThat(sit.entry().getValue(), is(expected[i][2]));
      i++;
    }

    assertThat(i, is(expected.length));
  }

  @Test
  public void testCompactRange2() throws Exception {
    final int now = GungnirUtils.currentTimeSecs();
    MemoryTable table = new MemoryTable(3, 4);
    table.compactRange();

    table.incr("aaa", now + 2);
    table.incr("aaa", now + 1);
    table.incr("bbb", now + 2);
    table.incr("aaa", now + 2);
    table.incr("aaa", now + 1);
    table.incr("aaa", now + 5);
    table.incr("aaa", now + 2);
    table.incr("bbb", now + 5);
    table.incr("aaa", now + 1);
    table.incr("aaa", now + 2);
    table.incr("bbb", now + 5);
    table.incr("aaa", now + 5);
    table.incr("bbb", now + 5);

    new MockUp<GungnirUtils>() {

      @Mock
      public int currentTimeSecs() {
        return now + 6;
      }
    };

    Object[][] expected = {
        {"aaa", now + 4, new byte[] {0, 0, 0, 3}},
        {"aaa", now + 5, new byte[] {0, 0, 0, 4}},
        {"bbb", now + 5, new byte[] {0, 0, 0, 1}},
    };
    List<Entry> entries = table.compactRange();
    int i = 0;
    for (Entry entry : entries) {
      assertThat(entry.getHashKey(), is(expected[i][0]));
      assertThat(entry.getTimeKey(), is(expected[i][1]));
      assertThat(entry.getValue(), is(expected[i][2]));
      i++;
    }

    assertThat(i, is(expected.length));
    assertThat(table.size(), is(5));

    table.incr("aaa", now + 1);
    table.incr("aaa", now + 2);
    table.incr("aaa", now + 5);
    table.incr("ccc", now + 4);
    table.incr("aaa", now + 4);

    new MockUp<GungnirUtils>() {

      @Mock
      public int currentTimeSecs() {
        return now + 9;
      }
    };

    expected = new Object[][] {
        {"bbb", now + 8, new byte[] {0, 0, 0, 3}},
        {"ccc", now + 7, new byte[] {0, 0, 0, 1}}
    };
    entries = table.compactRange();
    i = 0;
    for (Entry entry : entries) {
      assertThat(entry.getHashKey(), is(expected[i][0]));
      assertThat(entry.getTimeKey(), is(expected[i][1]));
      assertThat(entry.getValue(), is(expected[i][2]));
      i++;
    }

    assertThat(i, is(expected.length));
    assertThat(table.size(), is(4));

    expected = new Object[][] {
        {"aaa", now + 7, new byte[] {0, 0, 0, 1}},
        {"aaa", now + 8, new byte[] {0, 0, 0, 3}}
    };
    entries = table.compactRange();
    i = 0;
    for (Entry entry : entries) {
      assertThat(entry.getHashKey(), is(expected[i][0]));
      assertThat(entry.getTimeKey(), is(expected[i][1]));
      assertThat(entry.getValue(), is(expected[i][2]));
      i++;
    }

    assertThat(i, is(expected.length));
    assertThat(table.size(), is(0));
  }

  @Test
  public void testCompactRange3() throws Exception {
    final int now = GungnirUtils.currentTimeSecs();
    MemoryTable table = new MemoryTable(3, 4);
    table.compactRange();

    table.put(now + 3, new byte[] {1});
    table.put(now + 1, new byte[] {2});
    table.put(now + 1, new byte[] {3});
    table.put(now + 3, new byte[] {4});
    table.put(now + 2, new byte[] {5});
    table.put(now + 2, new byte[] {6});

    assertThat(table.size(), is(6));

    new MockUp<GungnirUtils>() {

      @Mock
      public int currentTimeSecs() {
        return now + 5;
      }
    };

    Object[][] expected = {
        {now + 4, new byte[] {2}},
        {now + 4, new byte[] {3}}
    };
    List<Entry> entries = table.compactRange();
    int i = 0;
    for (Entry entry : entries) {
      assertThat(entry.getTimeKey(), is(expected[i][0]));
      assertThat(entry.getValue(), is(expected[i][1]));
      i++;
    }

    assertThat(i, is(expected.length));
    assertThat(table.size(), is(4));

    expected = new Object[][] {
        {null, now + 5, new byte[] {5}},
        {null, now + 5, new byte[] {6}},
        {null, now + 6, new byte[] {1}},
        {null, now + 6, new byte[] {4}}
    };
    i = 0;
    for (Iterator<Entry> it = table.iterator(); it.hasNext();) {
      Entry entry = it.next();
      assertThat(entry.getHashKey(), is(expected[i][0]));
      assertThat(entry.getTimeKey(), is(expected[i][1]));
      assertThat(entry.getValue(), is(expected[i][2]));
      i++;
    }

    assertThat(i, is(expected.length));

    entries = table.compactRange();
    assertThat(entries.isEmpty(), is(true));

    table.put(now + 5, new byte[] {7});
    table.put(now + 5, new byte[] {8});

    assertThat(table.removeFirst().getValue(), is(new byte[] {5}));

    entries = table.compactRange();
    assertThat(entries.isEmpty(), is(true));

    assertThat(table.removeFirst().getValue(), is(new byte[] {6}));
    assertThat(table.removeFirst().getValue(), is(new byte[] {1}));

    new MockUp<GungnirUtils>() {

      @Mock
      public int currentTimeSecs() {
        return now + 10;
      }
    };

    expected = new Object[][] {
        {now + 6, new byte[] {4}},
        {now + 8, new byte[] {7}},
        {now + 8, new byte[] {8}}
    };
    entries = table.compactRange();
    i = 0;
    for (Entry entry : entries) {
      assertThat(entry.getTimeKey(), is(expected[i][0]));
      assertThat(entry.getValue(), is(expected[i][1]));
      i++;
    }

    assertThat(table.size(), is(0));
  }

  // TODO Class cannot be created (missing no-arg constructor): com.google.common.collect.HashBiMap
  //@Test
  public void testSerialize() throws Exception {
    memoryTable.put("aaa", 3, new byte[] {1, 1, 1});
    memoryTable.put("aaa", 1, new byte[] {2, 2, 2});
    memoryTable.put("bbb", 1, new byte[] {3, 3, 3});
    memoryTable.put("aaa", 1, new byte[] {4, 4, 4});
    memoryTable.put("aaa", 2, new byte[] {5, 5, 5});
    memoryTable.put("ccc", 2, new byte[] {6, 6, 6});
    memoryTable.put("aaa", 3, new byte[] {7, 7, 7});
    memoryTable.put("bbb", 3, new byte[] {8, 8, 8});
    memoryTable.put("aaa", 5, new byte[] {9, 9, 9});
    memoryTable.put("ccc", 1, new byte[] {10, 10, 10});

    KryoSerializer serializer = new KryoSerializer();
    serializer.register(UnsignedBytes.lexicographicalComparator().getClass());
    byte[] bytes = serializer.serialize(memoryTable);

    MemoryTable table = serializer.deserialize(bytes, MemoryTable.class);

    byte[][] expected = {
        {2, 2, 2},
        {4, 4, 4},
        {5, 5, 5},
        {1, 1, 1},
        {7, 7, 7},
        {9, 9, 9}
    };
    int i = 0;
    for (Iterator<Entry> it = table.iterator("aaa"); it.hasNext();) {
      Entry entry = it.next();
      assertThat(entry.getValue(), is(expected[i]));
      i++;
    }

    assertThat(i, is(expected.length));
    assertThat(table.size(), is(10));
    assertThat(table.byteSize(), is(30));
    assertThat(table.size("aaa"), is(6));
    assertThat(table.isEmpty("aaa"), is(false));
    assertThat(table.size("bbb"), is(2));
    assertThat(table.isEmpty("bbb"), is(false));

    memoryTable.clear();
  }
}
