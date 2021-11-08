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

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.standalone.types.StructType;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Source;
import org.apache.pulsar.io.core.SourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A source connector that read data from delta lake through delta standalone reader.
 */
@Getter(AccessLevel.PACKAGE)
public class DeltaLakeConnectorSource implements Source<GenericRecord> {
    private static final Logger log = LoggerFactory.getLogger(DeltaLakeConnectorSource.class);
    private final Integer minCheckpointMapKey = -1;
    public DeltaLakeConnectorConfig config;
    // delta lake schema, when delta lake schema changes,this schema will change
    private SourceContext sourceContext;
    private ExecutorService executor;
    private ExecutorService parseParquetExecutor;
    private LinkedBlockingQueue<DeltaRecord> queue = new LinkedBlockingQueue<DeltaRecord>();
    private long topicPartitionNum;
    private final Map<Integer, DeltaCheckpoint> checkpointMap = new HashMap<Integer, DeltaCheckpoint>();
    public DeltaReader reader;
    private String destinationTopic;
    private long readCnt = 0;
    private long sendCnt = 0;

    public void setDeltaSchema(StructType deltaSchema) {
        log.info("update pulsar schema from {} to {}", this.deltaSchema.getTreeString(), deltaSchema.getTreeString());
        this.deltaSchema = deltaSchema;
    }

    private StructType deltaSchema;
    private GenericSchema<GenericRecord> pulsarSchema;

    @Override
    public void open(Map<String, Object> map, SourceContext sourceContext) throws Exception {
        if (null != config) {
            throw new IllegalStateException("Connector is already open");
        }
        this.sourceContext = sourceContext;
        this.destinationTopic = sourceContext.getOutputTopic();
        log.info("destination topic is {} numberInstances: {} myInstanceId: {}",
                this.destinationTopic, sourceContext.getNumInstances(), sourceContext.getInstanceId());

        // load the configuration and validate it
        this.config = DeltaLakeConnectorConfig.load(map);
        this.config.validate();

        CompletableFuture<List<String>> listPartitions =
                sourceContext.getPulsarClient().getPartitionsForTopic(sourceContext.getOutputTopic());
        this.topicPartitionNum = listPartitions.get().size();
        // try to open the delta lake
        reader = new DeltaReader(config);

        Optional<Map<Integer, DeltaCheckpoint>> checkpointMapOpt = getCheckpointFromStateStore(sourceContext);
        if (!checkpointMapOpt.isPresent()) {
            log.info("instanceId:{} source connector do nothing, without any partition assigned",
                    sourceContext.getInstanceId());
            return;
        }
        checkpointMap.forEach((key, value) -> {
            DeltaRecord.msgSeqCntMap.put(key, value.getSeqCount());
        });
        reader.setFilter(initDeltaReadFilter(checkpointMap));
        reader.setStartCheckpoint(checkpointMap.get(minCheckpointMapKey));
        DeltaReader.topicPartitionNum = this.topicPartitionNum;
        parseParquetExecutor = Executors.newFixedThreadPool(3);
        reader.setExecutorService(parseParquetExecutor);

        executor = Executors.newFixedThreadPool(1);
        executor.execute(new DeltaReaderThread(this, parseParquetExecutor));
    }

    @Override
    public Record<GenericRecord> read() throws Exception {
        readCnt++;
        DeltaRecord deltaRecord = this.queue.take();
        return deltaRecord;
    }

    @Override
    public void close() throws Exception {
    }

    public void enqueue(DeltaReader.RowRecordData rowRecordData) {
        try {
            log.debug("enqueue : {} {} [{}] putcount:{} readcount {}",
                    rowRecordData.nextCursor.toString(), this.destinationTopic,
                    rowRecordData.simpleGroup.toString(), sendCnt++, readCnt);
            if (this.deltaSchema != null) {
                this.queue.put(new DeltaRecord(rowRecordData, this.destinationTopic, deltaSchema, this.sourceContext));
            } else if (this.pulsarSchema != null) {
                this.queue.put(new DeltaRecord(rowRecordData, this.destinationTopic, pulsarSchema, this.sourceContext));
            }
        } catch (Exception ex) {
            log.error("delta message enqueue interrupted", ex);
        }
    }


    /**
     * get checkpoint position from pulsar function stateStore.
     * @return if this instance not own any partition, will return empty, else return the checkpoint map.
     */
    Optional<Map<Integer, DeltaCheckpoint>> getCheckpointFromStateStore(SourceContext sourceContext)
            throws Exception {
        int instanceId = sourceContext.getInstanceId();
        int numInstance = sourceContext.getNumInstances();
        DeltaCheckpoint checkpoint = null;

        List<Integer> partitionList = new LinkedList<Integer>();
        for (int i = 0; i < topicPartitionNum; i++) {
            log.info("partition: {} numInstance {} instanceId {}", i, numInstance, instanceId);
            if (i % numInstance == instanceId) {
                partitionList.add(i);
            }
        }

        if (partitionList.size() <= 0) {
            return Optional.empty();
        }

        for (int i = 0; i < partitionList.size(); i++) {
            ByteBuffer byteBuffer = null;
            try {
                log.info("begin to get checkpoint from pulsar");
                byteBuffer = sourceContext.getState(DeltaCheckpoint.getStatekey(partitionList.get(i)));
                if (byteBuffer == null) {
                    log.info("get checkpoint for partition {} ,return empty, will start from first",
                            partitionList.get(i));
                    continue;
                }
            } catch (Exception e) {
                log.error("getState exception failed for partition {} , ", i, e);
                throw new Exception("get checkpoint from state store failed for partition " + i);
            }
            String jsonString = Charset.forName("utf-8").decode(byteBuffer).toString();
            ObjectMapper mapper = new ObjectMapper();
            DeltaCheckpoint tmpCheckpoint = null;
            try {
                mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
                tmpCheckpoint = mapper.readValue(jsonString, DeltaCheckpoint.class);
            } catch (Exception e) {
                log.error("parse the checkpoint for partition {} failed, jsonString: {}",
                        partitionList.get(i), jsonString, e);
                throw new Exception("parse checkpoint failed");
            }
            checkpointMap.put(partitionList.get(i), tmpCheckpoint);
            if (checkpoint == null || checkpoint.compareTo(tmpCheckpoint) > 0) {
                checkpoint = tmpCheckpoint;
            }
        }

        if (checkpointMap.size() == 0) {
            Long startVersion = Long.valueOf(0);
            if (!this.config.startingVersion.equals("")) {
                startVersion = reader.getAndValidateSnapShotVersion(this.config.startingSnapShotVersionNumber);
            } else if (!this.config.startingTimeStamp.equals("")) {
                startVersion = reader.getSnapShotVersionFromTimeStamp(this.config.startingTimeStampSecond);
            }

            if (this.config.includeHistoryData) {
                checkpoint = new DeltaCheckpoint(DeltaCheckpoint.StateType.FULL_COPY, startVersion);
            } else {
                checkpoint = new DeltaCheckpoint(DeltaCheckpoint.StateType.INCREMENTAL_COPY, startVersion);
            }
            checkpointMap.put(minCheckpointMapKey, checkpoint);
            try {
                deltaSchema = reader.getSnapShot(startVersion).getMetadata().getSchema();
            } catch (Exception e) {
                log.error("getSchema from snapshot {} failed, ", startVersion, e);
                Class<?> classType = sourceContext.getClass();
                Field adminField = classType.getDeclaredField("pulsarAdmin");
                adminField.setAccessible(true);
                PulsarAdmin admin = (PulsarAdmin) adminField.get(sourceContext);
                pulsarSchema = Schema.generic(admin.schemas().getSchemaInfo(sourceContext.getOutputTopic()));
                log.info("get latest schema from pulsar, get {}, pulsarSchema {}",
                        admin.schemas().getSchemaInfo(sourceContext.getOutputTopic()),
                        pulsarSchema);
            }
            for (int i = 0; i < partitionList.size(); i++) {
                log.info("checkpointMap not including partition {}, will start from first {}",
                        partitionList.get(i), checkpoint.toString());
                checkpointMap.put(partitionList.get(i), checkpoint);
            }
        } else {
            Long startVersion = reader.getAndValidateSnapShotVersion(checkpoint.getSnapShotVersion());
            if (startVersion > checkpoint.getSnapShotVersion()) {
                log.error("checkpoint version: {} not exist, current version {}",
                        checkpoint.getSnapShotVersion(), startVersion);
                throw new Exception("last checkpoint version not exist, need to handle this manually");
            }
            try {
                deltaSchema = reader.getSnapShot(startVersion).getMetadata().getSchema();
            } catch (Exception e) {
                log.error("getSchema from snapshot {} failed, ", startVersion, e);
                Class<?> classType = sourceContext.getClass();
                Field adminField = classType.getDeclaredField("pulsarAdmin");
                adminField.setAccessible(true);
                PulsarAdmin admin = (PulsarAdmin) adminField.get(sourceContext);
                pulsarSchema = Schema.generic(admin.schemas().getSchemaInfo(sourceContext.getOutputTopic()));
                log.info("get latest schema from pulsar, get {}, pulsarSchema {}",
                        admin.schemas().getAllSchemas(sourceContext.getOutputTopic()).get(0),
                        pulsarSchema);
            }
            checkpointMap.put(minCheckpointMapKey, checkpoint);
            for (int i = 0; i < partitionList.size(); i++) {
                if (!checkpointMap.containsKey(partitionList.get(i))) {
                    log.warn("checkpointMap not including partition {}, will use default checkpoint {}",
                            partitionList.get(i), checkpoint.toString());
                    checkpointMap.put(partitionList.get(i), checkpoint);
                }
            }
        }

        return Optional.of(checkpointMap);
    }

    private Function<DeltaReader.ReadCursor, Boolean> initDeltaReadFilter(Map<Integer, DeltaCheckpoint> partitionMap) {
        return (readCursor) -> {
            if (readCursor == null) {
                log.info("readCursor is null return true");
                return true;
            }
            long slot = DeltaReader.getPartitionIdByDeltaPartitionValue(readCursor.partitionValue,
                    this.topicPartitionNum);
            if (!partitionMap.containsKey(Integer.valueOf((int) slot))) {
                log.info("partitionMap {} not includeing {}", partitionMap, slot);
                return false;
            }

            DeltaCheckpoint newCheckPoint = null;
            if (readCursor.isFullSnapShot) {
                newCheckPoint = new DeltaCheckpoint(DeltaCheckpoint.StateType.FULL_COPY, readCursor.version);
            } else {
                newCheckPoint = new DeltaCheckpoint(DeltaCheckpoint.StateType.INCREMENTAL_COPY, readCursor.version);
            }
            newCheckPoint.setMetadataChangeFileIndex(readCursor.changeIndex);

            DeltaCheckpoint s = this.checkpointMap.get(Integer.valueOf((int) slot));
            if (s == null) { // no checkpoint before means there are no record before, no need to filter
                log.info("checkpoint for partition {} {} is missing", this.checkpointMap, slot);
                return true;
            }
            if (s != null && newCheckPoint.compareTo(s) >= 0) {
                return true;
            }
            return false;
        };
    }


    public DeltaCheckpoint getMinCheckpoint() {
        return this.checkpointMap.get(minCheckpointMapKey);
    }
}