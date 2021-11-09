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

import io.delta.standalone.actions.Metadata;
import java.util.List;
import java.util.concurrent.ExecutorService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The delta reader thread class for {@link DeltaLakeConnectorSource}.
 */
public class DeltaReaderThread extends Thread {
    private static final Logger log = LoggerFactory.getLogger(DeltaReaderThread.class);
    private final DeltaLakeConnectorSource source;
    private boolean stopped;
    private ExecutorService parseParquetExecutor;

    public DeltaReaderThread(DeltaLakeConnectorSource source, ExecutorService executor) {
        this.source = source;
        this.stopped = false;
        this.parseParquetExecutor = executor;
    }


    public void run() {
        DeltaReader reader = this.source.reader;
        DeltaCheckpoint checkpoint = source.getMinCheckpoint();
        Long startVersion = checkpoint.getSnapShotVersion();
        while (!stopped) {
            try {
                log.debug("begin to read version {} ", startVersion);
                List<DeltaReader.ReadCursor> actionList = reader.getDeltaActionFromSnapShotVersion(
                        startVersion, checkpoint.isFullCopy());
                if (actionList.size() == 0) {
                    if (startVersion.equals(checkpoint.getSnapShotVersion())
                            && checkpoint.getMetadataChangeFileIndex() > 0 && checkpoint.getRowNum() > 0) {
                        log.info("read end of the delta version {}, will go to next version {}",
                                startVersion, startVersion + 1);
                        startVersion = startVersion + 1;
                        continue;
                    } else {
                        log.debug("read from version: {}  not find any delta actions, wait to get actions next round",
                                startVersion, actionList.size(), startVersion + 1);
                        Thread.sleep(1000 * 10);
                        continue;
                    }
                }
                for (int i = 0; i < actionList.size(); i++) {
                    if (actionList.get(i).act instanceof Metadata) {
                        source.setDeltaSchema(((Metadata) actionList.get(i).act).getSchema());
                        continue;
                    }
                    if (i == actionList.size() - 1) {
                        DeltaReader.ReadCursor cursor = actionList.get(i);
                        cursor.endOfVersion = true;
                    }
                    List<DeltaReader.RowRecordData> rowRecords = reader.readParquetFile(actionList.get(i));
                    log.debug("version {} actionIndex: {} rowRecordSize {}", startVersion, i, rowRecords.size());
                    rowRecords.forEach(source::enqueue);
                }
                startVersion++;
            } catch (Exception ex) {
                log.error("read data from delta lake error.", ex);
                close();
            }
        }
    }

    public void close() {
        stopped = true;
    }
}
