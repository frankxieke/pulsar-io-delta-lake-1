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

import io.delta.standalone.actions.CommitInfo;
import io.delta.standalone.actions.Metadata;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The delta reader thread class for {@link DeltaLakeConnectorSource}.
 */
public class DeltaReaderThread extends Thread {
    private static final Logger log = LoggerFactory.getLogger(DeltaReaderThread.class);
    private final DeltaLakeConnectorSource source;
    private boolean stopped;
    private ExecutorService readExecutor;
    private static long maxReadActionSizeOneRound;
    private AtomicInteger processingException;

    public DeltaReaderThread(DeltaLakeConnectorSource source, ExecutorService executor,
                             long maxReadActionSizeOneRound, AtomicInteger processingException) {
        this.source = source;
        this.stopped = false;
        this.readExecutor = executor;
        this.maxReadActionSizeOneRound = maxReadActionSizeOneRound;
        this.processingException = processingException;
    }

    public void run() {
        DeltaReader reader = this.source.reader;
        DeltaCheckpoint checkpoint = source.getMinCheckpoint();
        Long nextVersion = checkpoint.getSnapShotVersion();
        Long startVersion = nextVersion;
        CompletableFuture<List<DeltaReader.ReadCursor>> actionListFuture = null;
        List<DeltaReader.ReadCursor> actionList = null;
        boolean isFullCopyPeriod = checkpoint.isFullCopy();
        LinkedBlockingQueue<DeltaReader.RowRecordData> queue =
                new LinkedBlockingQueue<DeltaReader.RowRecordData>(source.config.maxReadRowCountOneRound);
        while (!stopped) {
            try {
                startVersion = nextVersion;
                log.debug("begin to read version {} ", startVersion);
                if (actionListFuture == null) {
                    actionList = reader.getDeltaActionFromSnapShotVersion(
                            startVersion, maxReadActionSizeOneRound, isFullCopyPeriod);
                } else {
                    actionList = actionListFuture.get();
                    actionListFuture = null;
                }
                if (actionList.size() == 0) {
                    if (startVersion.equals(checkpoint.getSnapShotVersion())
                            && checkpoint.getMetadataChangeFileIndex() >= 0 && checkpoint.getRowNum() >= 0) {
                        log.info("read end of the delta version {}, will go to next version {}",
                                startVersion, startVersion + 1);
                        nextVersion = startVersion + 1;
                        continue;
                    } else {
                        log.debug("read from version: {}  not find any delta actions, wait to get actions next round",
                                startVersion, actionList.size(), startVersion + 1);
                        Thread.sleep(1000 * 10);
                        continue;
                    }
                }

                // get the last action snapshot version as the nextVersion
                nextVersion = actionList.get(actionList.size() - 1).getVersion() + 1;
                if (isFullCopyPeriod) {
                    log.info("begin to change from fullCopy mode to incrCopy mode, incrCopy version {}", nextVersion);
                    isFullCopyPeriod = false;
                }
                actionListFuture = reader.getDeltaActionFromSnapShotVersionAsync(nextVersion,
                        maxReadActionSizeOneRound, isFullCopyPeriod);

                int base = 0;
                while (base < actionList.size()) {
                    int concurrency = reader.getMaxConcurrency(actionList, base);
                    log.debug("totalActionSize {} concurency {} base {}", actionList.size(), concurrency, base);
                    if (concurrency > 1) { // read multiple files
                        long start = System.currentTimeMillis();
                        List<CompletableFuture<List<DeltaReader.RowRecordData>>> futureList = new ArrayList<>();
                        for (int j = 0; j < concurrency; j++) {
                            int index = base + j;
                            if (actionList.get(index).act instanceof Metadata) {
                                source.setDeltaSchema(((Metadata) actionList.get(index).act).getSchema());
                                continue;
                            }
                            CompletableFuture<List<DeltaReader.RowRecordData>> rowRecordsFuture =
                                    reader.readTotalParquetFileAsync(actionList.get(index), readExecutor);
                            futureList.add(rowRecordsFuture);
                        }
                        long totalSize = 0;
                        for (int k = 0; k < futureList.size(); k++) {
                            CompletableFuture<List<DeltaReader.RowRecordData>> rowRecordsFuture = futureList.get(k);
                            List<DeltaReader.RowRecordData> rowRecords = rowRecordsFuture.get();
                            rowRecords.forEach(source::enqueue);
                            totalSize += rowRecords.size();
                            log.debug("version {} actionIndex: {} actionSize {} rowRecordSize {}",
                                    actionList.get(base + k).getVersion(),
                                    base + k, actionList.size(), rowRecords.size());
                        }

                        long end = System.currentTimeMillis();
                        log.debug("parse all files cost {} ms, total record: {} totalFiles {}",
                                end - start, totalSize, futureList.size());
                    } else {
                        AtomicInteger readStatus = new AtomicInteger(0);
                        if (actionList.get(base).act instanceof CommitInfo) {
                            base = base + concurrency;
                            continue;
                        }
                        reader.readPartParquetFileAsync(actionList.get(base), readExecutor, queue, readStatus);
                        while (true) {
                            int queueSize = queue.size();
                            if (queueSize > 0) {
                                for (int q = 0; q < queueSize; q++) {
                                    DeltaReader.RowRecordData record = queue.take();
                                    source.enqueue(record);
                                }
                            } else if (readStatus.get() == 1) {
                                break;
                            } else if (readStatus.get() < 0) {
                                log.error("readPartParquetFileAsync encounter exception, will skip read");
                                throw new IOException("readPartParquetFileAsync failed");
                            }
                        }
                    }
                    base = base + concurrency;
                }

            } catch (InterruptedException | ExecutionException | IOException ex) {
                log.error("read data from delta lake error, will mark processingException", ex);
                close();
                this.processingException.incrementAndGet();
            } catch (Exception e) {
                log.error("read data from delta lake error, will mark processingException", e);
                close();
                this.processingException.incrementAndGet();
            }
        }
    }

    public void close() {
        stopped = true;
    }
}
