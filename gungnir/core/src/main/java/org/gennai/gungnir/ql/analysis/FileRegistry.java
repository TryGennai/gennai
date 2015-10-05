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

package org.gennai.gungnir.ql.analysis;

import static org.gennai.gungnir.GungnirConfig.*;
import static org.gennai.gungnir.GungnirConst.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.apache.storm.guava.collect.Maps;
import org.gennai.gungnir.GungnirManager;
import org.gennai.gungnir.UserEntity;
import org.gennai.gungnir.metastore.MetaStore;
import org.gennai.gungnir.metastore.MetaStoreException;
import org.gennai.gungnir.metastore.NotStoredException;
import org.gennai.gungnir.ql.FileStat;
import org.gennai.gungnir.ql.session.StatementEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class FileRegistry {

  private static Logger LOG = LoggerFactory.getLogger(FileRegistry.class);

  private String cacheDir;
  private int retryTimes;
  private int retryInterval;
  private Map<String, FileStat> filesMap;
  private MetaStore metaStore;

  public FileRegistry(StatementEntity statement) {
    cacheDir = statement.getConfig().getString(LOCAL_DIR) + "/" + SESSION_CACHE_DIR + "/"
        + statement.getSessionId() + "/" + statement.getStatementId();
    retryTimes = statement.getConfig().getInteger(EXPORT_RETRY_TIMES);
    retryInterval = statement.getConfig().getInteger(EXPORT_RETRY_INTERVAL);
  }

  public String getCacheDir() {
    return cacheDir;
  }

  private boolean exportFile(FileStat fileStat) throws MetaStoreException, IOException {
    try {
      Path exportFile = Paths.get(cacheDir, fileStat.getName());
      if (Files.exists(exportFile)) {
        Files.deleteIfExists(exportFile);
      }
      if (!metaStore.exportFile(fileStat, cacheDir, retryTimes, retryInterval)) {
        LOG.warn("'{}' is locked", fileStat.getName());
        return false;
      }
      return true;
    } catch (NotStoredException e) {
      LOG.warn("'{}' isn't registered", fileStat.getName(), e);
      return false;
    }
  }

  public void exportFiles(UserEntity owner) throws IOException, MetaStoreException {
    Map<String, FileStat> newFilesMap = null;

    if (metaStore == null) {
      metaStore = GungnirManager.getManager().getMetaStore();
    }

    List<FileStat> files = metaStore.findFiles(owner);
    if (!files.isEmpty()) {
      newFilesMap = Maps.newHashMap();
    }

    for (FileStat file : files) {
      if (filesMap == null) {
        if (exportFile(file)) {
          if (newFilesMap == null) {
            newFilesMap = Maps.newHashMap();
          }
          newFilesMap.put(file.getName(), file);
        }
      } else {
        FileStat regFile = filesMap.remove(file.getName());
        if (regFile == null) {
          if (exportFile(file)) {
            newFilesMap.put(file.getName(), file);
          }
        } else {
          if (!file.getCreateTime().equals(regFile.getCreateTime())) {
            if (exportFile(file)) {
              newFilesMap.put(file.getName(), file);
            }
          } else {
            newFilesMap.put(file.getName(), file);
          }
        }
      }
    }

    if (filesMap != null) {
      for (FileStat file : filesMap.values()) {
        Files.deleteIfExists(Paths.get(cacheDir, file.getName()));
      }
    }

    filesMap = newFilesMap;
  }

  public List<FileStat> getFiles() {
    if (filesMap != null) {
      return Lists.newArrayList(filesMap.values());
    }
    return null;
  }
}
