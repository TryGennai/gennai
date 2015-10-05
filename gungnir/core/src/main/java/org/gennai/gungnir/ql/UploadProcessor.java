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

package org.gennai.gungnir.ql;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.metastore.AlreadyStoredException;
import org.gennai.gungnir.metastore.MetaStore;
import org.gennai.gungnir.metastore.MetaStore.ChunkIterator;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.metastore.NotStoredException;
import org.gennai.gungnir.ql.session.StatementEntity;
import org.gennai.gungnir.ql.task.TaskExecuteException;

public final class UploadProcessor {

  private UploadProcessor() {
  }

  public static void beginUpload(StatementEntity statement, String fileName)
      throws UploadProcessorException {
    try {
      MetaStore metaStore = GungnirManager.getManager().getMetaStore();
      FileStat fileStat = new FileStat();
      fileStat.setName(fileName);
      fileStat.setOwner(statement.getOwner());
      metaStore.insertFile(fileStat);

      statement.setUploadingFileName(fileName);
    } catch (MetaStoreException e) {
      throw new UploadProcessorException(e);
    } catch (AlreadyStoredException e) {
      throw new UploadProcessorException(e);
    }
  }

  public static void writeChunk(StatementEntity statement, ByteBuffer chunk)
      throws UploadProcessorException {
    MetaStore metaStore = null;
    FileStat fileStat = null;
    try {
      metaStore = GungnirManager.getManager().getMetaStore();
      fileStat = metaStore.findFile(statement.getUploadingFileName(), statement.getOwner());
      byte[] bytes = new byte[chunk.remaining()];
      chunk.get(bytes);
      metaStore.insertChunk(fileStat, bytes);
    } catch (MetaStoreException e) {
      throw new UploadProcessorException(e);
    } catch (NotStoredException e) {
      throw new UploadProcessorException(e);
    }
  }

  public static void finishUpload(StatementEntity statement, int fileSize, long checksum)
      throws UploadProcessorException {
    try {
      MetaStore metaStore = GungnirManager.getManager().getMetaStore();
      FileStat fileStat = metaStore.findFile(statement.getUploadingFileName(),
          statement.getOwner());

      int size = 0;
      CRC32 crc32 = new CRC32();
      ChunkIterator it = null;
      try {
        for (it = metaStore.findChunks(fileStat); it.hasNext();) {
          byte[] bytes = it.next();
          size += bytes.length;
          crc32.update(bytes);
        }
      } finally {
        it.close();
      }

      if (size != fileSize) {
        metaStore.deleteChunks(fileStat);
        metaStore.deleteFile(fileStat);
        throw new UploadProcessorException(new TaskExecuteException("Incorrect size"));
      }

      if (crc32.getValue() != checksum) {
        metaStore.deleteChunks(fileStat);
        metaStore.deleteFile(fileStat);
        throw new UploadProcessorException(new TaskExecuteException("Invalid checksum"));
      }

      fileStat.setSize(fileSize);
      fileStat.setChecksum(checksum);
      metaStore.changeFileStat(fileStat);

      statement.setUploadingFileName(null);
    } catch (MetaStoreException e) {
      throw new UploadProcessorException(e);
    } catch (NotStoredException e) {
      throw new UploadProcessorException(e);
    }
  }
}
