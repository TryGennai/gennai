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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.gennai.gungnir.utils.GungnirUtils;
import org.gennai.gungnir.utils.IntArrayUtils;
import org.gennai.gungnir.utils.KryoSerializer;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.UnsignedBytes;

public final class RocksDBTable {

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
  private ColumnFamilyDescriptor storeDescriptor;
  private ColumnFamilyDescriptor hashKeyDescriptor;
  private ColumnFamilyDescriptor hashIndexDescriptor;
  private ColumnFamilyDescriptor hashMetaDescriptor;
  private DBOptions dbOptions;
  private RocksDB db;
  private ColumnFamilyHandle storeHandle;
  private ColumnFamilyHandle hashKeyHandle;
  private ColumnFamilyHandle hashIndexHandle;
  private ColumnFamilyHandle hashMetaHandle;
  private int lastHashIndex = 0;
  private int size;
  private Entry seekEntry;
  private IntArrayUtils i3 = new IntArrayUtils(3);
  private IntArrayUtils i1 = new IntArrayUtils(1);
  private KryoSerializer serializer = new KryoSerializer();
  private Cache<Object, byte[]> keyBytesCache;
  private Cache<byte[], Object> keyCache;

  private void loadMetaData() {
    RocksIterator it = db.newIterator(hashMetaHandle);
    try {
      for (it.seekToFirst(); it.isValid(); it.next()) {
        int hashIndex = i1.get(it.key(), 0);
        if (hashIndex >= lastHashIndex) {
          lastHashIndex = hashIndex + 1;
        }
        size += i1.get(it.value(), 0);
      }
    } finally {
      it.dispose();
    }
  }

  private RocksDBTable(String dbPath, int expireSecs, int seekSize) throws RocksDBException {
    this.expireSecs = expireSecs;
    this.seekSize = seekSize;

    storeDescriptor = new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY,
        new ColumnFamilyOptions());
    hashKeyDescriptor = new ColumnFamilyDescriptor("hash_key", new ColumnFamilyOptions());
    hashIndexDescriptor = new ColumnFamilyDescriptor("hash_index", new ColumnFamilyOptions());
    hashMetaDescriptor = new ColumnFamilyDescriptor("hash_meta", new ColumnFamilyOptions());

    List<ColumnFamilyDescriptor> columnFamilyDescriptors = Lists.newArrayListWithCapacity(4);
    columnFamilyDescriptors.add(storeDescriptor);
    columnFamilyDescriptors.add(hashKeyDescriptor);
    columnFamilyDescriptors.add(hashIndexDescriptor);
    columnFamilyDescriptors.add(hashMetaDescriptor);

    dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);

    List<ColumnFamilyHandle> columnFamilyHandles = Lists.newArrayListWithCapacity(4);
    db = RocksDB.open(dbOptions, dbPath, columnFamilyDescriptors, columnFamilyHandles);
    storeHandle = columnFamilyHandles.get(0);
    hashKeyHandle = columnFamilyHandles.get(1);
    hashIndexHandle = columnFamilyHandles.get(2);
    hashMetaHandle = columnFamilyHandles.get(3);

    loadMetaData();

    serializer = new KryoSerializer();
    serializer.register(HashKey.class, new Serializer<HashKey>() {

      @Override
      public void write(Kryo kryo, Output output, HashKey hashKey) {
        if (hashKey.key instanceof List) {
          kryo.writeObject(output, HashKey.Type.COMPOSITE);
          kryo.writeObject(output, hashKey.key);
        } else {
          kryo.writeObject(output, HashKey.Type.SINGLE);
          kryo.writeClassAndObject(output, hashKey.key);
        }
      }

      @Override
      public HashKey read(Kryo kryo, Input input, Class<HashKey> type) {
        HashKey.Type keyType = kryo.readObject(input, HashKey.Type.class);
        Object key = null;
        if (keyType == HashKey.Type.COMPOSITE) {
          key = kryo.readObject(input, ArrayList.class);
        } else {
          key = kryo.readClassAndObject(input);
        }
        return new HashKey(key);
      }
    });
  }

  public static RocksDBTable open(String dbPath) throws RocksDBException {
    return new RocksDBTable(dbPath, 0, 0);
  }

  public static RocksDBTable open(String dbPath, int expireSecs, int seekSize)
      throws RocksDBException {
    return new RocksDBTable(dbPath, expireSecs, seekSize);
  }

  private void updateMetaData(int hashIndex, int sz, WriteBatch writeBatch)
      throws RocksDBException {
    if (hashIndex >= 0) {
      int hashSize;
      byte[] indexBytes = i1.create(hashIndex);
      byte[] sizeBytes = db.get(hashMetaHandle, indexBytes);
      if (sizeBytes != null) {
        hashSize = i1.get(sizeBytes, 0);
      } else {
        hashSize = 0;
      }
      hashSize += sz;
      if (hashSize > 0) {
        writeBatch.put(hashMetaHandle, indexBytes, i1.create(hashSize));
      } else {
        writeBatch.remove(hashMetaHandle, indexBytes);
        byte[] keyBytes = db.get(hashIndexHandle, indexBytes);
        writeBatch.remove(hashIndexHandle, indexBytes);
        writeBatch.remove(hashKeyHandle, keyBytes);
      }
    }
    size += sz;
  }

  private int expire(int hashIndex, WriteBatch writeBatch) {
    int sz = 0;
    if (expireSecs > 0) {
      RocksIterator it = db.newIterator(storeHandle);
      if (hashIndex >= 0) {
        it.seek(i3.create(hashIndex, 0, 0));
      } else {
        it.seekToFirst();
      }

      int now = GungnirUtils.currentTimeSecs();
      for (; it.isValid(); it.next()) {
        int[] ikey = i3.get(it.key());
        if (ikey[0] == hashIndex && ikey[1] < now) {
          if (ikey[2] >= 0) {
            sz++;
          } else {
            sz += i1.get(it.value(), 0);
          }

          writeBatch.remove(storeHandle, it.key());
        } else {
          break;
        }
      }

      it.dispose();
    }
    return sz;
  }

  static final class HashKey {

    enum Type {
      SINGLE, COMPOSITE
    }

    private Object key;

    private HashKey(Object key) {
      this.key = key;
    }
  }

  public void setKeyCache(int cacheMaxSize, int cacheExpireSec) {
    keyBytesCache = CacheBuilder.newBuilder().maximumSize(cacheMaxSize)
        .expireAfterAccess(cacheExpireSec, TimeUnit.SECONDS).build();
    keyCache = CacheBuilder.newBuilder().maximumSize(cacheMaxSize)
        .expireAfterAccess(cacheExpireSec, TimeUnit.SECONDS).build();
  }

  private byte[] serializeHashKey(final Object hashKey) {
    if (keyBytesCache == null) {
      return serializer.serialize(new HashKey(hashKey));
    } else {
      try {
        return keyBytesCache.get(hashKey, new Callable<byte[]>() {
          @Override
          public byte[] call() throws Exception {
            return serializer.serialize(new HashKey(hashKey));
          }
        });
      } catch (ExecutionException e) {
        return serializer.serialize(new HashKey(hashKey));
      }
    }
  }

  private Object deserHashKey(final byte[] keyBytes) {
    if (keyCache == null) {
      return serializer.deserialize(keyBytes, HashKey.class).key;
    } else {
      try {
        return keyCache.get(keyBytes, new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            return serializer.deserialize(keyBytes, HashKey.class).key;
          }
        });
      } catch (ExecutionException e) {
        return serializer.deserialize(keyBytes, HashKey.class).key;
      }
    }
  }

  private byte[] floorKey(byte[] key) {
    byte[] lastKey = null;
    RocksIterator it = db.newIterator(storeHandle);
    it.seek(key);
    if (it.isValid()) {
      byte[] currentKey = it.key();
      if (UnsignedBytes.lexicographicalComparator().compare(currentKey, key) <= 0) {
        lastKey = currentKey;
      } else {
        it.prev();
        if (it.isValid()) {
          lastKey = it.key();
        }
      }
    } else {
      it.seekToLast();
      if (it.isValid()) {
        lastKey = it.key();
      }
    }
    it.dispose();
    return lastKey;
  }

  public void put(Object hashKey, int timeKey, byte[] value) throws RocksDBException {
    WriteBatch writeBatch = new WriteBatch();

    Integer hashIndex = null;
    if (hashKey != null) {
      byte[] keyBytes = serializeHashKey(hashKey);
      byte[] indexBytes = db.get(hashKeyHandle, keyBytes);
      if (indexBytes != null) {
        hashIndex = i1.get(indexBytes, 0);
      } else {
        hashIndex = lastHashIndex;
        indexBytes = i1.create(hashIndex);
        writeBatch.put(hashKeyHandle, keyBytes, indexBytes);
        writeBatch.put(hashIndexHandle, indexBytes, keyBytes);
        lastHashIndex++;
      }
    } else {
      hashIndex = -1;
    }

    timeKey += expireSecs;
    byte[] key = i3.create(hashIndex, timeKey, Integer.MAX_VALUE);
    byte[] lastKey = floorKey(key);
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

    writeBatch.put(key, value);

    int sz = expire(hashIndex, writeBatch);
    sz = 1 - sz;
    updateMetaData(hashIndex, sz, writeBatch);

    WriteOptions writeOptions = new WriteOptions();
    try {
      db.write(writeOptions, writeBatch);
    } finally {
      writeBatch.dispose();
      writeOptions.dispose();
    }
  }

  public void put(int timeKey, byte[] value) throws RocksDBException {
    put(null, timeKey, value);
  }

  public int incr(Object hashKey, int timeKey) throws RocksDBException {
    WriteBatch writeBatch = new WriteBatch();

    Integer hashIndex = null;
    if (hashKey != null) {
      byte[] keyBytes = serializeHashKey(hashKey);
      byte[] indexBytes = db.get(hashKeyHandle, keyBytes);
      if (indexBytes != null) {
        hashIndex = i1.get(indexBytes, 0);
      } else {
        hashIndex = lastHashIndex;
        indexBytes = i1.create(hashIndex);
        writeBatch.put(hashKeyHandle, keyBytes, indexBytes);
        writeBatch.put(hashIndexHandle, indexBytes, keyBytes);
        lastHashIndex++;
      }
    } else {
      hashIndex = -1;
    }

    timeKey += expireSecs;
    int v = 0;
    byte[] key = i3.create(hashIndex, timeKey, -1);
    byte[] value = db.get(storeHandle, key);
    if (value == null) {
      v = 1;
    } else {
      v = i1.incr(value, 0);
    }

    writeBatch.put(key, i1.create(v));

    int sz = expire(hashIndex, writeBatch);
    sz = 1 - sz;
    updateMetaData(hashIndex, sz, writeBatch);

    expire(hashIndex, writeBatch);

    WriteOptions writeOptions = new WriteOptions();
    try {
      db.write(writeOptions, writeBatch);
    } finally {
      writeBatch.dispose();
      writeOptions.dispose();
    }

    return v;
  }

  public void incr(int timeKey) throws RocksDBException {
    incr(null, timeKey);
  }

  public int size(Object hashKey) throws RocksDBException {
    Integer hashIndex = null;
    byte[] indexBytes = null;
    if (hashKey != null) {
      byte[] keyBytes = serializeHashKey(hashKey);
      indexBytes = db.get(hashKeyHandle, keyBytes);
      if (indexBytes != null) {
        hashIndex = i1.get(indexBytes, 0);
      } else {
        return 0;
      }
    } else {
      hashIndex = -1;
    }

    int sz;
    if (hashIndex >= 0) {
      sz = i1.get(db.get(hashMetaHandle, indexBytes), 0);
    } else {
      sz = size;
    }
    if (expireSecs > 0) {
      RocksIterator it = db.newIterator(storeHandle);
      it.seek(i3.create(hashIndex, 0, 0));
      int now = GungnirUtils.currentTimeSecs();
      for (; it.isValid(); it.next()) {
        int[] ikey = i3.get(it.key());
        if (ikey[0] == hashIndex && ikey[1] < now) {
          sz--;
        } else {
          break;
        }
      }
      it.dispose();
    }
    return sz;
  }

  public int size() throws RocksDBException {
    return size(null);
  }

  public boolean isEmpty(Object hashKey) throws RocksDBException {
    return size(hashKey) == 0;
  }

  public boolean isEmpty() throws RocksDBException {
    return size(null) == 0;
  }

  private static class HashMetaData {

    private int size;
  }

  public final class EntryIterator {

    private Object hashKey;
    private Integer hashIndex;
    private RocksIterator it = db.newIterator(storeHandle);
    private byte[] currentKey;
    private Entry current;
    private WriteBatch writeBatch;
    private Map<Integer, HashMetaData> hashMetaMap;

    private EntryIterator() {
      hashIndex = -1;
      if (expireSecs > 0) {
        it.seek(i3.create(-1, GungnirUtils.currentTimeSecs(), 0));
      } else {
        it.seekToFirst();
      }
    }

    private EntryIterator(Object hashKey) throws RocksDBException {
      this.hashKey = hashKey;
      byte[] indexBytes = db.get(hashKeyHandle, serializeHashKey(hashKey));
      if (indexBytes != null) {
        hashIndex = i1.get(indexBytes, 0);
        if (expireSecs > 0) {
          it.seek(i3.create(hashIndex, GungnirUtils.currentTimeSecs(), 0));
        } else {
          it.seek(i3.create(hashIndex, 0, 0));
        }
      }
    }

    public boolean hasNext() {
      if (hashIndex == null) {
        return false;
      }
      if (!it.isValid()) {
        return false;
      }
      if (hashKey != null) {
        return hashIndex == i3.get(it.key(), 0);
      }
      return true;
    }

    public Entry next() throws RocksDBException {
      if (hashIndex == null) {
        return null;
      }

      currentKey = it.key();
      int[] ikey = i3.get(currentKey);
      if (hashIndex >= 0) {
        byte[] keyBytes = db.get(hashIndexHandle, i1.create(ikey[0]));
        current = new Entry(deserHashKey(keyBytes), ikey[1], ikey[2], it.value());
      } else {
        current = new Entry(null, ikey[1], ikey[2], it.value());
      }
      hashIndex = ikey[0];
      it.next();
      return current;
    }

    public void remove() throws RocksDBException {
      if (current != null) {
        if (writeBatch == null) {
          writeBatch = new WriteBatch();
        }

        if (hashMetaMap == null) {
          hashMetaMap = Maps.newHashMap();
        }
        HashMetaData hashMetaData = hashMetaMap.get(hashIndex);
        if (hashMetaData == null) {
          hashMetaData = new HashMetaData();
          hashMetaMap.put(hashIndex, hashMetaData);
        }
        if (current.getSeqNo() >= 0) {
          hashMetaData.size++;
        } else {
          hashMetaData.size += i1.get(current.getValue(), 0);
        }

        writeBatch.remove(storeHandle, currentKey);
      }
    }

    private void commit() throws RocksDBException {
      if (writeBatch != null) {
        for (Map.Entry<Integer, HashMetaData> entry : hashMetaMap.entrySet()) {
          updateMetaData(entry.getKey(), -entry.getValue().size, writeBatch);
        }

        WriteOptions writeOptions = new WriteOptions();
        try {
          db.write(writeOptions, writeBatch);
        } finally {
          writeBatch.dispose();
          writeOptions.dispose();
          writeBatch = null;
          hashMetaMap = null;
        }
      }
    }

    public void close() throws RocksDBException {
      commit();
      it.dispose();
    }
  }

  public EntryIterator iterator(Object hashKey) throws RocksDBException {
    return new EntryIterator(hashKey);
  }

  public EntryIterator iterator() {
    return new EntryIterator();
  }

  public final class SeekIterator {

    private RocksIterator it = db.newIterator(storeHandle);
    private Entry current;
    private Integer hashIndex;
    private byte[] currentKey;
    private WriteBatch writeBatch;
    private Map<Integer, HashMetaData> hashMetaMap;

    private SeekIterator() {
    }

    public void seekToFirst() throws RocksDBException {
      it.seekToFirst();
    }

    public void seek(Object hashKey, int timeKey, int seqNo, boolean inclusive)
        throws RocksDBException {
      if (hashKey != null) {
        byte[] indexBytes = db.get(hashKeyHandle, serializeHashKey(hashKey));
        if (indexBytes != null) {
          byte[] key = i3.create(i1.get(indexBytes, 0), timeKey, seqNo);
          it.seek(key);
          if (!inclusive && it.isValid()
              && UnsignedBytes.lexicographicalComparator().compare(it.key(), key) == 0) {
            it.next();
          }
        } else {
          it.seek(i3.create(-1, -1, -1));
        }
      } else {
        byte[] key = i3.create(-1, timeKey, seqNo);
        it.seek(key);
        if (!inclusive && it.isValid()
            && UnsignedBytes.lexicographicalComparator().compare(it.key(), key) == 0) {
          it.next();
        }
      }
    }

    public boolean isValid() {
      return it.isValid();
    }

    public void next() throws RocksDBException {
      it.next();
    }

    public Entry entry() throws RocksDBException {
      currentKey = it.key();
      int[] ikey = i3.get(currentKey);
      if (ikey[0] >= 0) {
        byte[] keyBytes = db.get(hashIndexHandle, i1.create(ikey[0]));
        current = new Entry(deserHashKey(keyBytes), ikey[1], ikey[2], it.value());
      } else {
        current = new Entry(null, ikey[1], ikey[2], it.value());
      }
      hashIndex = ikey[0];
      return current;
    }

    public void remove() throws RocksDBException {
      if (current != null) {
        if (writeBatch == null) {
          writeBatch = new WriteBatch();
        }

        if (hashMetaMap == null) {
          hashMetaMap = Maps.newHashMap();
        }
        HashMetaData hashMetaData = hashMetaMap.get(hashIndex);
        if (hashMetaData == null) {
          hashMetaData = new HashMetaData();
          hashMetaMap.put(hashIndex, hashMetaData);
        }
        if (current.getSeqNo() >= 0) {
          hashMetaData.size++;
        } else {
          hashMetaData.size += i1.get(current.getValue(), 0);
        }

        writeBatch.remove(storeHandle, currentKey);
      }
    }

    private void commit() throws RocksDBException {
      if (writeBatch != null) {
        for (Map.Entry<Integer, HashMetaData> entry : hashMetaMap.entrySet()) {
          updateMetaData(entry.getKey(), -entry.getValue().size, writeBatch);
        }

        WriteOptions writeOptions = new WriteOptions();
        try {
          db.write(writeOptions, writeBatch);
        } finally {
          writeBatch.dispose();
          writeOptions.dispose();
          writeBatch = null;
          hashMetaMap = null;
        }
      }
    }

    public void close() throws RocksDBException {
      commit();
      it.dispose();
    }
  }

  public SeekIterator seekIterator() {
    return new SeekIterator();
  }

  public Entry removeFirst(Object hashKey) throws RocksDBException {
    Integer hashIndex = null;
    if (hashKey != null) {
      byte[] keyBytes = serializeHashKey(hashKey);
      byte[] indexBytes = db.get(hashKeyHandle, keyBytes);
      if (indexBytes != null) {
        hashIndex = i1.get(indexBytes, 0);
      } else {
        return null;
      }
    } else {
      hashIndex = -1;
    }

    Entry removedEntry = null;
    RocksIterator it = db.newIterator(storeHandle);
    if (hashIndex >= 0) {
      it.seek(i3.create(hashIndex, 0, 0));
    } else {
      it.seekToFirst();
    }

    WriteBatch writeBatch = null;
    int now = GungnirUtils.currentTimeSecs();
    int sz = 0;
    for (; it.isValid(); it.next()) {
      int[] ikey = i3.get(it.key());
      if (ikey[0] != hashIndex) {
        break;
      }

      if (writeBatch == null) {
        writeBatch = new WriteBatch();
      }

      if (ikey[2] >= 0) {
        sz++;
      } else {
        sz += i1.get(it.value(), 0);
      }

      if (expireSecs > 0 && ikey[1] < now) {
        writeBatch.remove(storeHandle, it.key());
      } else {
        removedEntry = new Entry(hashKey, ikey[1], ikey[2], it.value());
        writeBatch.remove(storeHandle, it.key());
        break;
      }
    }

    try {
      if (writeBatch != null) {
        updateMetaData(hashIndex, -sz, writeBatch);

        WriteOptions writeOptions = new WriteOptions();
        try {
          db.write(writeOptions, writeBatch);
        } finally {
          writeBatch.dispose();
          writeOptions.dispose();
        }
      }
    } finally {
      it.dispose();
    }

    return removedEntry;
  }

  public Entry removeFirst() throws RocksDBException {
    return removeFirst(null);
  }

  public List<Entry> compactRange() throws RocksDBException {
    SeekIterator it = seekIterator();

    if (seekEntry == null) {
      it.seekToFirst();
    } else {
      it.seek(seekEntry.getHashKey(), seekEntry.getTimeKey(), seekEntry.getSeqNo(),
          false);
      if (!it.isValid()) {
        it.seekToFirst();
      }
    }

    int now = GungnirUtils.currentTimeSecs();
    List<Entry> expiredEntries = Lists.newArrayList();

    for (int i = 0; it.isValid() && i < seekSize; i++) {
      seekEntry = it.entry();
      if (seekEntry.getTimeKey() < now) {
        expiredEntries.add(seekEntry);
        it.remove();
        it.next();
      } else {
        seekEntry.setTimeKey(Integer.MAX_VALUE);
        seekEntry.setSeqNo(Integer.MAX_VALUE);
        it.seek(seekEntry.getHashKey(), seekEntry.getTimeKey(), seekEntry.getSeqNo(),
            false);
      }
    }

    it.close();

    return expiredEntries;
  }

  public void close() throws RocksDBException {
    if (storeHandle != null) {
      storeHandle.dispose();
    }
    if (hashKeyHandle != null) {
      hashKeyHandle.dispose();
    }
    if (hashIndexHandle != null) {
      hashIndexHandle.dispose();
    }
    if (hashMetaHandle != null) {
      hashMetaHandle.dispose();
    }
    if (db != null) {
      db.close();
    }
    if (dbOptions != null) {
      dbOptions.dispose();
    }
  }
}
