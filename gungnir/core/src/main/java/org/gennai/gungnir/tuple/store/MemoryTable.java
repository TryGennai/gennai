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

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

import org.gennai.gungnir.utils.GungnirUtils;
import org.gennai.gungnir.utils.IntArrayUtils;

import com.google.common.collect.HashBiMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;

public class MemoryTable {

  private static class HashMetaData {

    private int size;
  }

  public static class Entry {

    private Object hashKey;
    private int timeKey;
    private int seqNo;
    private byte[] value;

    public Entry(Object hashKey, int timeKey, int seqNo, byte[] value) {
      this.hashKey = hashKey;
      this.timeKey = timeKey;
      this.seqNo = seqNo;
      this.value = value;
    }

    public void setHashKey(Object hashKey) {
      this.hashKey = hashKey;
    }

    public Object getHashKey() {
      return hashKey;
    }

    public void setTimeKey(int timeKey) {
      this.timeKey = timeKey;
    }

    public int getTimeKey() {
      return timeKey;
    }

    public void setSeqNo(int seqNo) {
      this.seqNo = seqNo;
    }

    public int getSeqNo() {
      return seqNo;
    }

    public byte[] getValue() {
      return value;
    }
  }

  private int expireSecs;
  private int seekSize;
  private NavigableMap<byte[], byte[]> delegate = Maps.newTreeMap(
      UnsignedBytes.lexicographicalComparator());
  private HashBiMap<Object, Integer> hashKeyMap = HashBiMap.create();
  private int lastHashIndex = 0;
  private Map<Integer, HashMetaData> hashMetaMap = Maps.newHashMap();
  private int startTime = Integer.MAX_VALUE;
  private int size;
  private int byteSize;
  private transient SeekIterator seekIterator;
  private transient Entry seekEntry;
  private transient IntArrayUtils i3 = new IntArrayUtils(3);
  private transient IntArrayUtils i1 = new IntArrayUtils(1);

  public MemoryTable() {
  }

  public MemoryTable(int expireSecs, int seekSize) {
    this.expireSecs = expireSecs;
    this.seekSize = seekSize;
    seekIterator = new SeekIterator();
  }

  public int getStartTime() {
    return startTime;
  }

  public int byteSize() {
    return byteSize;
  }

  private static class MetaData {

    private int size;
    private int byteSize;
  }

  private void updateMetaData(int hashIndex, MetaData metaData) {
    if (hashIndex >= 0) {
      HashMetaData hashMetaData = hashMetaMap.get(hashIndex);
      if (hashMetaData == null) {
        hashMetaData = new HashMetaData();
        hashMetaMap.put(hashIndex, hashMetaData);
      }

      hashMetaData.size += metaData.size;
      if (hashMetaData.size == 0) {
        hashMetaMap.remove(hashIndex);
        hashKeyMap.inverse().remove(hashIndex);
      }
    }

    size += metaData.size;
    byteSize += metaData.byteSize;
  }

  private MetaData expire(int hashIndex) {
    MetaData metaData = new MetaData();
    if (expireSecs > 0) {
      Iterator<Map.Entry<byte[], byte[]>> it = null;
      if (hashIndex >= 0) {
        it = delegate.subMap(i3.create(hashIndex, 0, 0), true, i3.create(hashIndex,
            GungnirUtils.currentTimeSecs() - 1, -1), true).entrySet().iterator();
      } else {
        it = delegate.headMap(i3.create(hashIndex, GungnirUtils.currentTimeSecs() - 1, -1),
            true).entrySet().iterator();
      }

      for (; it.hasNext();) {
        Map.Entry<byte[], byte[]> entry = it.next();
        int[] ikey = i3.get(entry.getKey());
        if (ikey[2] >= 0) {
          metaData.size++;
          metaData.byteSize += entry.getValue().length;
        } else {
          metaData.size += i1.get(entry.getValue(), 0);
          metaData.byteSize += 4;
        }

        it.remove();
      }
    }
    return metaData;
  }

  public void put(Object hashKey, int timeKey, byte[] value) {
    Integer hashIndex = null;
    if (hashKey != null) {
      hashIndex = hashKeyMap.get(hashKey);
      if (hashIndex == null) {
        hashIndex = lastHashIndex;
        hashKeyMap.put(hashKey, hashIndex);
        lastHashIndex++;
      }
    } else {
      hashIndex = -1;
    }

    timeKey += expireSecs;
    byte[] key = i3.create(hashIndex, timeKey, Integer.MAX_VALUE);
    byte[] lastKey = delegate.floorKey(key);
    if (lastKey == null) {
      i3.set(key, 2, 0);
    } else {
      int[] ikey = i3.get(lastKey);
      if (ikey[0] == hashIndex && ikey[1] == timeKey) {
        i3.set(key, 2, ikey[2] + 1);
      } else {
        i3.set(key, 2, 0);
      }
    }

    delegate.put(key, value);

    if (timeKey < startTime) {
      startTime = timeKey;
    }

    MetaData metaData = expire(hashIndex);
    metaData.size = 1 - metaData.size;
    metaData.byteSize = value.length - metaData.byteSize;
    updateMetaData(hashIndex, metaData);
  }

  public void put(int timeKey, byte[] value) {
    put(null, timeKey, value);
  }

  public int incr(Object hashKey, int timeKey) {
    Integer hashIndex = null;
    if (hashKey != null) {
      hashIndex = hashKeyMap.get(hashKey);
      if (hashIndex == null) {
        hashIndex = lastHashIndex;
        hashKeyMap.put(hashKey, hashIndex);
        lastHashIndex++;
      }
    } else {
      hashIndex = -1;
    }

    timeKey += expireSecs;
    int v = 0;
    int bsz = 0;
    byte[] key = i3.create(hashIndex, timeKey, -1);
    byte[] value = delegate.get(key);
    if (value == null) {
      delegate.put(key, i1.create(1));
      v = 1;
      bsz = 4;
    } else {
      v = i1.incr(value, 0);
    }

    if (timeKey < startTime) {
      startTime = timeKey;
    }

    MetaData metaData = expire(hashIndex);
    metaData.size = 1 - metaData.size;
    metaData.byteSize = bsz - metaData.byteSize;
    updateMetaData(hashIndex, metaData);

    return v;
  }

  public void incr(int timeKey) {
    incr(null, timeKey);
  }

  public int size(Object hashKey) {
    Integer hashIndex = null;
    if (hashKey != null) {
      hashIndex = hashKeyMap.get(hashKey);
      if (hashIndex == null) {
        return 0;
      }
    } else {
      hashIndex = -1;
    }

    int sz;
    if (hashIndex >= 0) {
      sz = hashMetaMap.get(hashIndex).size;
    } else {
      sz = size;
    }
    if (expireSecs > 0) {
      sz -= delegate.subMap(i3.create(hashIndex, 0, 0), true, i3.create(hashIndex,
          GungnirUtils.currentTimeSecs() - 1, -1), true).size();
    }
    return sz;
  }

  public int size() {
    return size(null);
  }

  public boolean isEmpty(Object hashKey) {
    return size(hashKey) == 0;
  }

  public boolean isEmpty() {
    return size(null) == 0;
  }

  private final class EntryIterator implements Iterator<Entry> {

    private Integer hashIndex;
    private Iterator<Map.Entry<byte[], byte[]>> it;
    private Entry current;

    private EntryIterator() {
      hashIndex = -1;
      if (expireSecs > 0) {
        it = delegate.tailMap(i3.create(-1, GungnirUtils.currentTimeSecs(), 0), true).entrySet()
            .iterator();
      } else {
        it = delegate.entrySet().iterator();
      }
    }

    private EntryIterator(Object hashKey) {
      hashIndex = hashKeyMap.get(hashKey);
      if (hashIndex != null) {
        if (expireSecs > 0) {
          it = delegate.subMap(i3.create(hashIndex, GungnirUtils.currentTimeSecs(), 0), true,
              i3.create(hashIndex, Integer.MAX_VALUE, -1), true).entrySet().iterator();
        } else {
          it = delegate.subMap(i3.create(hashIndex, 0, 0), true,
              i3.create(hashIndex, Integer.MAX_VALUE, -1), true).entrySet().iterator();
        }
      }
    }

    @Override
    public boolean hasNext() {
      if (it == null) {
        return false;
      }
      return it.hasNext();
    }

    @Override
    public Entry next() {
      if (it == null) {
        return null;
      }
      Map.Entry<byte[], byte[]> entry = it.next();
      int[] ikey = i3.get(entry.getKey());
      if (ikey[0] >= 0) {
        current = new Entry(hashKeyMap.inverse().get(ikey[0]), ikey[1], ikey[2], entry.getValue());
      } else {
        current = new Entry(null, ikey[1], ikey[2], entry.getValue());
      }
      hashIndex = ikey[0];
      return current;
    }

    @Override
    public void remove() {
      if (current != null) {
        MetaData metaData = new MetaData();
        if (current.getSeqNo() >= 0) {
          metaData.size = -1;
          metaData.byteSize = -current.getValue().length;
        } else {
          metaData.size = -i1.get(current.getValue(), 0);
          metaData.byteSize = -4;
        }
        updateMetaData(hashIndex, metaData);

        it.remove();
      }
    }
  }

  public Iterator<Entry> iterator(Object hashKey) {
    return new EntryIterator(hashKey);
  }

  public Iterator<Entry> iterator() {
    return new EntryIterator();
  }

  public final class SeekIterator {

    private NavigableMap<byte[], byte[]> subMap;
    private Iterator<Map.Entry<byte[], byte[]>> it;
    private Entry current;
    private Integer hashIndex;

    private SeekIterator() {
    }

    public void next() {
      if (it != null && it.hasNext()) {
        Map.Entry<byte[], byte[]> entry = it.next();
        int[] ikey = i3.get(entry.getKey());
        if (ikey[0] >= 0) {
          current = new Entry(hashKeyMap.inverse().get(ikey[0]), ikey[1], ikey[2],
              entry.getValue());
        } else {
          current = new Entry(null, ikey[1], ikey[2], entry.getValue());
        }
        hashIndex = ikey[0];
      } else {
        current = null;
        hashIndex = null;
      }
    }

    public void seekToFirst() {
      subMap = delegate;
      it = subMap.entrySet().iterator();
      next();
    }

    public void seek(Object hashKey, int timeKey, int seqNo, boolean inclusive) {
      if (subMap == null) {
        subMap = delegate;
      }

      if (hashKey != null) {
        Integer index = hashKeyMap.get(hashKey);
        if (index == null) {
          it = null;
        } else {
          subMap = subMap.tailMap(i3.create(index, timeKey, seqNo), inclusive);
          it = subMap.entrySet().iterator();
          next();
        }
      } else {
        subMap = subMap.tailMap(i3.create(-1, timeKey, seqNo), inclusive);
        it = subMap.entrySet().iterator();
        next();
      }
    }

    public boolean isValid() {
      return current != null;
    }

    public Entry entry() {
      return current;
    }

    public void remove() {
      if (current != null) {
        MetaData metaData = new MetaData();
        if (current.getSeqNo() >= 0) {
          metaData.size = -1;
          metaData.byteSize = -current.getValue().length;
        } else {
          metaData.size = -i1.get(current.getValue(), 0);
          metaData.byteSize = -4;
        }
        updateMetaData(hashIndex, metaData);

        it.remove();
      }
    }
  }

  public SeekIterator seekIterator() {
    return new SeekIterator();
  }

  public Entry removeFirst(Object hashKey) {
    Integer hashIndex = null;
    if (hashKey != null) {
      hashIndex = hashKeyMap.get(hashKey);
      if (hashIndex == null) {
        return null;
      }
    } else {
      hashIndex = -1;
    }

    Entry removedEntry = null;
    int now = GungnirUtils.currentTimeSecs();
    Iterator<Map.Entry<byte[], byte[]>> it = null;
    if (hashIndex >= 0) {
      it = delegate.subMap(i3.create(hashIndex, 0, 0), true,
          i3.create(hashIndex, Integer.MAX_VALUE, -1), true).entrySet().iterator();
    } else {
      it = delegate.entrySet().iterator();
    }

    MetaData metaData = new MetaData();
    for (; it.hasNext();) {
      Map.Entry<byte[], byte[]> entry = it.next();
      int[] ikey = i3.get(entry.getKey());

      if (ikey[2] >= 0) {
        metaData.size--;
        metaData.byteSize -= entry.getValue().length;
      } else {
        metaData.size -= i1.get(entry.getValue(), 0);
        metaData.byteSize -= 4;
      }

      if (expireSecs > 0 && ikey[1] < now) {
        it.remove();
      } else {
        removedEntry = new Entry(hashKey, ikey[1], ikey[2], entry.getValue());
        it.remove();
        break;
      }
    }

    if (metaData.size < 0) {
      updateMetaData(hashIndex, metaData);
    }

    return removedEntry;
  }

  public Entry removeFirst() {
    return removeFirst(null);
  }

  public List<Entry> compactRange() {
    if (seekEntry == null) {
      seekIterator.seekToFirst();
    } else {
      seekIterator.seek(seekEntry.getHashKey(), seekEntry.getTimeKey(), seekEntry.getSeqNo(),
          false);
      if (!seekIterator.isValid()) {
        seekIterator.seekToFirst();
      }
    }

    int now = GungnirUtils.currentTimeSecs();
    List<Entry> expiredEntries = Lists.newArrayList();

    for (int i = 0; seekIterator.isValid() && i < seekSize; i++) {
      seekEntry = seekIterator.entry();
      if (seekEntry.getTimeKey() < now) {
        expiredEntries.add(seekEntry);
        seekIterator.remove();
        seekIterator.next();
      } else {
        seekEntry.setTimeKey(Integer.MAX_VALUE);
        seekEntry.setSeqNo(-1);
        seekIterator.seek(seekEntry.getHashKey(), seekEntry.getTimeKey(), seekEntry.getSeqNo(),
            false);
      }
    }

    return expiredEntries;
  }

  public void clear() {
    delegate = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
    hashKeyMap = HashBiMap.create();
    hashMetaMap = Maps.newTreeMap();
    startTime = Integer.MAX_VALUE;
    size = 0;
    byteSize = 0;
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append('{');
    int i = 0;
    for (Map.Entry<byte[], byte[]> entry : delegate.entrySet()) {
      if (i > 0) {
        sb.append(", ");
      }
      sb.append(i3.toString(entry.getKey()));
      sb.append('=');
      sb.append(Arrays.toString(entry.getValue()));
      i++;
    }
    sb.append('}');
    return sb.toString();
  }
}
