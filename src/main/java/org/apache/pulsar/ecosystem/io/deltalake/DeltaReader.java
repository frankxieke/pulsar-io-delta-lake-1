/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pulsar.ecosystem.io.deltalake;

import io.delta.standalone.DeltaLog;
import io.delta.standalone.Snapshot;
import io.delta.standalone.VersionLog;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.CommitInfo;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.actions.RemoveFile;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import lombok.Data;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.schema.Type;
import org.apache.pulsar.ecosystem.io.deltalake.parquet.ParquetReaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The delta reader for {@link DeltaReaderThread}.
 */
@Data
public class DeltaReader {
    private static final Logger log = LoggerFactory.getLogger(DeltaReader.class);
    public DeltaCheckpoint startCheckpoint;
    public String tablePath;
    public DeltaLog deltaLog;
    public Function<ReadCursor, Boolean> filter;
    public static long topicPartitionNum;
    public ExecutorService executorService;
    public DeltaLakeConnectorConfig config;
    private Configuration conf;


    public static long getPartitionIdByDeltaPartitionValue(String partitionValue, long topicPartitionNum) {
        if (partitionValue == null) {
            partitionValue = "";
        }
        return Murmur32Hash.getInstance().makeHash(partitionValue.getBytes())
                % topicPartitionNum;
    }

    /**
     * The ReadCursor is used to describe the read position.
     */
    @Data
    public class ReadCursor implements Cloneable {
        Action act;
        Long version;
        Boolean isFullSnapShot;
        long changeIndex;
        long rowNum;
        boolean endOfFile;
        boolean endOfVersion;
        String partitionValue;

        public ReadCursor(Action act, Long version, Boolean isFullSnapShot,
                          long changeIndex, String partitionValue) {
            this.act = act;
            this.version = version;
            this.isFullSnapShot = isFullSnapShot;
            this.changeIndex = changeIndex;
            this.partitionValue = partitionValue;
            this.rowNum = 0;
        }
        public ReadCursor(Action act, Long version, Boolean isFullSnapShot,
                          long changeIndex, String partitionValue, long rowNum) {
            this.act = act;
            this.version = version;
            this.isFullSnapShot = isFullSnapShot;
            this.changeIndex = changeIndex;
            this.partitionValue = partitionValue;
            this.rowNum = rowNum;
        }

        @Override
        protected Object clone() throws CloneNotSupportedException {
            ReadCursor rc = null;
            try {
                rc = (ReadCursor) super.clone();
            } catch (CloneNotSupportedException e) {
                throw e;
            }
            return rc;
        }
    }

    /**
     * The RowRecordData is used to contain the data and next read position.
     */
    public static class RowRecordData {
        ReadCursor nextCursor;
        SimpleGroup simpleGroup;
        List<Type> parquetSchema;

        public RowRecordData(ReadCursor nextCursor, SimpleGroup simpleGroup) {
            this.nextCursor = nextCursor;
            this.simpleGroup = simpleGroup;
        }

        public RowRecordData(ReadCursor nextCursor, SimpleGroup simpleGroup,
                             List<Type> parquetSchema) {
            this.nextCursor = nextCursor;
            this.simpleGroup = simpleGroup;
            this.parquetSchema = parquetSchema;
        }
    }

    public DeltaReader(DeltaLakeConnectorConfig config) throws Exception {
        this.config = config;
        this.tablePath = this.config.tablePath;
        this.open();
    }


    public Long getSnapShotVersionFromTimeStamp(Long timeStamp) {
        Snapshot s = null;
        try {
            s = deltaLog.getSnapshotForTimestampAsOf(timeStamp);
            long v = s.getVersion();
            return v;
        } catch (Exception e) {
            try {
                s = deltaLog.update();
                long v = s.getVersion();
                log.error("timestamp  {} is not exist is delta lake, will use the latest version {}",
                        timeStamp, v);
                return v;
            } catch (Exception e2) {
                log.warn("get latest snapshot version failed, use 0 instead");
                return 0L;
            }
        }
    }

    public Long getAndValidateSnapShotVersion(Long snapShotVersion) {
        Snapshot s = null;
        try {
            if (snapShotVersion == -1) {
                try {
                    s = deltaLog.update();
                    long v = s.getVersion();
                    log.error("snapShotVersion {} is not exist is delta lake, will use the latest version {}",
                            snapShotVersion, v);
                    return v;
                } catch (Exception e) {
                    log.warn("get latest snapshot version failed, use 0 instead");
                    return 0L;
                }
            }
            s = deltaLog.getSnapshotForVersionAsOf(snapShotVersion);
            return s.getVersion();
        } catch (Exception e) {
            try {
                s = deltaLog.update();
                long v = s.getVersion();
                log.error("snapShotVersion {} is not exist is delta lake, will use the latest version {}",
                        snapShotVersion, v);
                return v;
            } catch (Exception e2) {
                log.warn("get latest snapshot version failed, use 0 instead");
                return 0L;
            }
        }
    }

    public Snapshot getSnapShot(Long snapShotVersion) {
        Snapshot s = deltaLog.getSnapshotForVersionAsOf(snapShotVersion);
        return s;
    }

    public CompletableFuture<List<ReadCursor>> getDeltaActionFromSnapShotVersionAsync(Long startVersion,
                                            Long maxActionSize, boolean isFullSnapshot) throws Exception {
        CompletableFuture<List<ReadCursor>> cf = new CompletableFuture<>();
        CompletableFuture.runAsync(() -> {
            try {
                List<ReadCursor> readCursorList =
                        getDeltaActionFromSnapShotVersion(startVersion, maxActionSize, isFullSnapshot);
                cf.complete(readCursorList);

            } catch (Exception e) {
                cf.completeExceptionally(e);
            }
        });
        return cf;
    }
    public List<ReadCursor> getDeltaActionFromSnapShotVersion(Long startVersion, Long maxActionSize,
                                                             boolean isFullSnapshot) {
        List<ReadCursor> actionList = new LinkedList<>();
        if (isFullSnapshot) {
            Snapshot snapshot = deltaLog.getSnapshotForVersionAsOf(startVersion);
            List<AddFile> addFiles = snapshot.getAllFiles();
            log.debug("allAddFile: {} startVersion: {}", addFiles, startVersion);
            for (int i = 0; i < addFiles.size(); i++) {
                AddFile add = addFiles.get(i);
                String partitionValue = partitionValueToString(add.getPartitionValues());
                ReadCursor cursor = new ReadCursor(add, startVersion, true, i, partitionValue);
                if (isMatch(cursor)) {
                    actionList.add(cursor);
                }
            }
        } else {
            long prevVersion = startVersion;
            long start = System.currentTimeMillis();
            Iterator<VersionLog> vlogs = deltaLog.getChanges(startVersion, false);
            while (vlogs.hasNext()) {
                VersionLog v = vlogs.next();
                if (v.getVersion() > prevVersion && actionList.size() >= maxActionSize) {
                    long end = System.currentTimeMillis();
                    log.debug("get changes {} cost {} ms total {} actions",
                            v.getVersion(), end - start, actionList.size());
                    break;
                }
                prevVersion = v.getVersion();
                List<Action> actions = v.getActions();
                for (int i = 0; i < actions.size(); i++) {
                    Action act = actions.get(i);
                    if (act instanceof AddFile) {
                        AddFile addFile = (AddFile) act;
                        String partitionValue = partitionValueToString(addFile.getPartitionValues());
                        ReadCursor cursor = new ReadCursor(addFile, v.getVersion(),
                                false, i, partitionValue);
                        Boolean matchFlag = isMatch(cursor);
                        log.debug("getChanges version: {} index: {} "
                                        + "actionListSize{} addFile {} dataChange {} partitionValue:{} "
                                        + "isMatch:{} modifiTime:{}",
                                v.getVersion(), i, actions.size(), addFile.getPath(),
                                addFile.isDataChange(),
                                partitionValue,
                                matchFlag,
                                addFile.getModificationTime());
                        if (matchFlag) {
                            actionList.add(cursor);
                        }
                    } else if (act instanceof CommitInfo) {
                        CommitInfo info = (CommitInfo) act;
                        log.debug("getChanges skip commitInfo version: {} index: {} Operation {} "
                                        + "operationParam {} modifiTime:{} operationMetrics:{}",
                                startVersion, i, info.getOperation(),
                                info.getOperationParameters().toString(),
                                info.getTimestamp(),
                                info.getOperationMetrics().get().toString());
                    } else if (act instanceof RemoveFile) {
                        RemoveFile removeFile = (RemoveFile) act;
                        String partitionValue = partitionValueToString(removeFile.getPartitionValues());
                        ReadCursor cursor = new ReadCursor(removeFile, v.getVersion(),
                                false, i, partitionValue);
                        Boolean matchFlag = isMatch(cursor);
                        log.debug("getChanges version: {} index: {} removeFile {} dataChange {} partitionValue:{} "
                                        + "isMatch:{} deletionTime:{}",
                                v.getVersion(), i, removeFile.getPath(),
                                removeFile.isDataChange(),
                                partitionValue, matchFlag,
                                removeFile.getDeletionTimestamp());
                        if (matchFlag) {
                            actionList.add(cursor);
                        }
                    } else if (act instanceof Metadata){
                        Metadata meta = (Metadata) act;
                            log.info("getChanges version: {} index: {} metadataChange schema:{} partitionColum:{}"
                                        + " format:{} createTime:{}",
                                v.getVersion(), i, meta.getSchema().getTreeString(),
                                meta.getPartitionColumns().toString(),
                                meta.getFormat(),
                                meta.getCreatedTime());
                        ReadCursor cursor = new ReadCursor(meta, v.getVersion(),
                                false, i, "");
                        actionList.add(cursor);
                    } else {
                        log.info("getChanges skip unknown change type: {}", act);
                    }
                }
            }
        }
        return actionList;
    }

    public CompletableFuture<List<RowRecordData>> readParquetFileAsync(ReadCursor startCursor,
                    ExecutorService executorService) throws Exception {
        CompletableFuture<List<RowRecordData>> cf = new CompletableFuture<>();
        CompletableFuture.runAsync(()-> {
            Action act = startCursor.act;
            List<RowRecordData> recordList = new LinkedList<>();
            String filePath;
            if (act instanceof AddFile) {
                if (config.fileSystemType.equals(config.FileSystemType)) {
                    filePath = config.tablePath + "/" + ((AddFile) act).getPath();
                } else {
                    filePath = config.tablePath + ((AddFile) act).getPath();
                }
                long start = System.currentTimeMillis();
                ParquetReaderUtils.Parquet parquet = null;
                try {
                    parquet = ParquetReaderUtils.getPargetParquetDataquetData(filePath, conf);
                } catch (IOException e) {
                    cf.completeExceptionally(e);
                    return;
                }
                log.debug("read and parse parquet file {} cost {} ms", filePath, System.currentTimeMillis() - start);
                for (int i = 0; i < parquet.getData().size(); i++) {
                    ReadCursor tmp;
                    try {
                        tmp = (ReadCursor) startCursor.clone();
                    } catch (CloneNotSupportedException e) {
                        cf.completeExceptionally(e);
                        return;
                    }
                    tmp.rowNum = i;
                    if (i == parquet.getData().size() - 1) {
                        tmp.endOfFile = true;
                    }
                    if (isMatch(tmp)) {
                        recordList.add(new RowRecordData(tmp, parquet.getData().get(i), parquet.getSchema()));
                    }
                }
            } else if (act instanceof RemoveFile) {
                if (config.fileSystemType.equals(config.FileSystemType)) {
                    filePath = config.tablePath + "/" + ((RemoveFile) act).getPath();
                } else {
                    filePath = config.tablePath + "/" + ((RemoveFile) act).getPath();
                }
                ParquetReaderUtils.Parquet parquet = null;
                try {
                    parquet = ParquetReaderUtils.getPargetParquetDataquetData (filePath, conf);
                } catch (IOException e) {
                    cf.completeExceptionally(e);
                    return;
                }
                for (int i = 0; i < parquet.getData().size(); i++) {
                    ReadCursor tmp = startCursor;
                    tmp.rowNum = i;
                    if (isMatch(tmp)) {
                        recordList.add(new RowRecordData(tmp, parquet.getData().get(i), parquet.getSchema()));
                    }
                }
            } else if (act instanceof CommitInfo) {
                ReadCursor tmp = startCursor;
                recordList.add(new RowRecordData(tmp, null));
            }
            cf.complete(recordList);
        }, executorService);
        return cf;
    }

    public static String partitionValueToString(Map<String, String> partitionValue) {
        TreeMap<String, String> treemap = new TreeMap<>(partitionValue);
        StringBuilder builder = new StringBuilder();
        for (Map.Entry<String, String> entry: treemap.entrySet()) {
            builder.append(entry.getKey());
            builder.append("=");
            builder.append(entry.getValue());
        }
        return builder.toString();
    }

    private void open() throws Exception {
        conf = new Configuration();
        if (config.fileSystemType.equals(config.S3Type)) {
            conf.set("fs.s3a.access.key", config.s3aAccesskey);
            conf.set("fs.s3a.secret.key", config.s3aSecretKey);
            conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        }
        deltaLog = DeltaLog.forTable(conf, tablePath);
    }

    private boolean isMatch(ReadCursor cursor) {
        if (filter.apply(cursor)) {
            return true;
        }
        return false;
    }
}
