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
import com.fasterxml.jackson.databind.ObjectMapper;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.RemoveFile;
import io.delta.standalone.types.BinaryType;
import io.delta.standalone.types.BooleanType;
import io.delta.standalone.types.ByteType;
import io.delta.standalone.types.DateType;
import io.delta.standalone.types.DecimalType;
import io.delta.standalone.types.DoubleType;
import io.delta.standalone.types.FloatType;
import io.delta.standalone.types.IntegerType;
import io.delta.standalone.types.LongType;
import io.delta.standalone.types.NullType;
import io.delta.standalone.types.ShortType;
import io.delta.standalone.types.StringType;
import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;
import io.delta.standalone.types.TimestampType;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Data;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.FieldSchemaBuilder;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericRecordBuilder;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaType;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.SourceContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A record wrapping an dataChange or metaChange message.
 */
@Data
public class DeltaRecord implements Record<GenericRecord> {
    private static final Logger log = LoggerFactory.getLogger(DeltaRecord.class);

    public static final String OP_FIELD = "op";
    public static final String PARTITION_VALUE_FIELD = "partition_value";
    public static final String CAPTURE_TS_FIELD = "capture_ts";
    public static final String TS_FIELD = "ts";

    public static final String OP_ADD_RECORD = "c";
    public static final String OP_DELETE_RECORD = "r";
    public static final String OP_META_SCHEMA = "m";

    private Map<String, String> properties;
    private GenericRecord value;
    private static GenericSchema<GenericRecord> s;
    private static StructType deltaSchema;
    private String topic;
    private DeltaReader.RowRecordData rowRecordData;
    public static Map<Integer, Long> msgSeqCntMap;
    public static Map<Integer, DeltaCheckpoint> saveCheckpointMap;
    private SourceContext sourceContext;
    private long sequence;
    private long partition;
    private String partitionValue;
    public static SaveCheckpointTread saveCheckpointTread;
    private AtomicInteger processingException;

    static class SaveCheckpointTread extends Thread {
        SourceContext sourceContext;

        public void setStopped(boolean stopped) {
            this.stopped = stopped;
        }

        boolean stopped;
        public SaveCheckpointTread(SourceContext sourceContext) {
            this.sourceContext = sourceContext;
            this.stopped = false;
        }


        @Override
        public void run() {
            Map<Integer, Long> lastSaveCheckpoint = new ConcurrentHashMap<>();
            if (saveCheckpointMap == null) {
                saveCheckpointMap = new ConcurrentHashMap<>();
            }
            while (!stopped) {
                long start = System.currentTimeMillis();
                saveCheckpointMap.forEach((k, v) -> {
                    Long lastSaveTm = lastSaveCheckpoint.getOrDefault(k, 0L);
                    Long currentTm = System.currentTimeMillis();
                    if (currentTm - lastSaveTm < 1000 * 60) {
                        return;
                    }

                    lastSaveCheckpoint.put(k, currentTm);
                    ObjectMapper mapper = new ObjectMapper();
                    try {
                        sourceContext.putState(DeltaCheckpoint.getStatekey(k),
                                ByteBuffer.wrap(mapper.writeValueAsBytes(v)));
                        log.debug("ack partition {} checkpoint {}", k, v.toString());
                    } catch (Exception e) {
                        log.error("putState failed for partition {} sequence {} ", k, e);
                    } finally {
                        long end = System.currentTimeMillis();
                        log.info("ack for partition {} checkpoint {} cost {} ms ", k, v,  end - start);
                    }
                });
                try {
                    Thread.sleep(1000);
                } catch (Exception e) {
                }
            }
        }
    }

    public DeltaRecord(DeltaReader.RowRecordData rowRecordData, String topic, StructType deltaSchema,
                       SourceContext sourceContext, AtomicInteger processingException) throws IOException {
        this.rowRecordData = rowRecordData;
        this.sourceContext = sourceContext;
        this.processingException = processingException;
        properties = new HashMap<>();
        this.topic = topic;
        if (this.deltaSchema != deltaSchema && deltaSchema != null) {
            this.deltaSchema = deltaSchema;
            this.s = convertToPulsarSchema(deltaSchema);
        }

        if (rowRecordData.nextCursor.act instanceof AddFile) {
            properties.put(OP_FIELD, OP_ADD_RECORD);
            partitionValue = DeltaReader.partitionValueToString(
                    ((AddFile) rowRecordData.nextCursor.act).getPartitionValues());
            properties.put(PARTITION_VALUE_FIELD, partitionValue);
            properties.put(CAPTURE_TS_FIELD, Long.toString(System.currentTimeMillis()));
            properties.put(TS_FIELD, Long.toString(((AddFile) rowRecordData.nextCursor.act).getModificationTime()));
            value = getGenericRecord(deltaSchema, null, rowRecordData);

        } else if (rowRecordData.nextCursor.act instanceof RemoveFile) {
            properties.put(OP_FIELD, OP_DELETE_RECORD);
            partitionValue = DeltaReader.partitionValueToString(
                            ((RemoveFile) rowRecordData.nextCursor.act).getPartitionValues());
            properties.put(PARTITION_VALUE_FIELD, partitionValue);
            properties.put(CAPTURE_TS_FIELD,  Long.toString(System.currentTimeMillis()));
            properties.put(TS_FIELD, Long.toString(((RemoveFile) rowRecordData.nextCursor.act).
                    getDeletionTimestamp().get()));
            value = getGenericRecord(deltaSchema, null, rowRecordData);
        } else {
            log.error("DeltaRecord: Not Support this kind of record {}", rowRecordData.nextCursor.act);
            throw new IOException("DeltaRecord: not support this kind of record");
        }

        String partitionValueStr = properties.get(PARTITION_VALUE_FIELD);
        partition = DeltaReader.getPartitionIdByDeltaPartitionValue(partitionValueStr,
                        DeltaReader.topicPartitionNum);
        if (msgSeqCntMap == null) {
            msgSeqCntMap = new ConcurrentHashMap<>();
        }
        if (saveCheckpointMap == null) {
            saveCheckpointMap = new ConcurrentHashMap<>();
        }
        Long msgCount = msgSeqCntMap.getOrDefault((int) partition, 0L);
        sequence = msgCount++;
        msgSeqCntMap.put((int) partition, msgCount);
    }

    public DeltaRecord(DeltaReader.RowRecordData rowRecordData, String topic, GenericSchema<GenericRecord> pulsarSchema,
                       SourceContext sourceContext, AtomicInteger processingException) throws IOException {
        this.s = pulsarSchema;
        this.sourceContext = sourceContext;
        this.processingException = processingException;
        properties = new HashMap<>();
        this.topic = topic;

        if (rowRecordData.nextCursor.act instanceof AddFile) {
            properties.put(OP_FIELD, OP_ADD_RECORD);
            partitionValue = DeltaReader.partitionValueToString(
                    ((AddFile) rowRecordData.nextCursor.act).getPartitionValues());
            properties.put(PARTITION_VALUE_FIELD, partitionValue);
            properties.put(CAPTURE_TS_FIELD, Long.toString(System.currentTimeMillis()));
            properties.put(TS_FIELD, Long.toString(((AddFile) rowRecordData.nextCursor.act).getModificationTime()));
            value = getGenericRecord(deltaSchema, pulsarSchema, rowRecordData);

        } else if (rowRecordData.nextCursor.act instanceof RemoveFile) {
            properties.put(OP_FIELD, OP_DELETE_RECORD);
            partitionValue = DeltaReader.partitionValueToString(
                    ((RemoveFile) rowRecordData.nextCursor.act).getPartitionValues());
            properties.put(PARTITION_VALUE_FIELD, partitionValue);
            properties.put(CAPTURE_TS_FIELD,  Long.toString(System.currentTimeMillis()));
            properties.put(TS_FIELD, Long.toString(((RemoveFile) rowRecordData.nextCursor.act).
                    getDeletionTimestamp().get()));
            value = getGenericRecord(deltaSchema, pulsarSchema, rowRecordData);
        } else {
            log.error("DeltaRecord: Not Support this kind of record {}", rowRecordData.nextCursor.act);
            throw new IOException("DeltaRecord: not support this kind of record");
        }
        String partitionValueStr = properties.get(PARTITION_VALUE_FIELD);

        if (msgSeqCntMap == null) {
            msgSeqCntMap = new ConcurrentHashMap<>();
        }
        if (saveCheckpointMap == null) {
            saveCheckpointMap = new ConcurrentHashMap<>();
        }
        partition = DeltaReader.getPartitionIdByDeltaPartitionValue(partitionValueStr,
                DeltaReader.topicPartitionNum);
        Long msgCount = msgSeqCntMap.getOrDefault(partition, 0L);
        sequence = msgCount++;
        msgSeqCntMap.put((int) partition, msgCount);
    }

    public static GenericSchema<GenericRecord> convertToPulsarSchema(StructType deltaSchema) throws IOException {
        // Try to transform schema
        RecordSchemaBuilder builder = SchemaBuilder
                .record(deltaSchema.getTypeName());
        FieldSchemaBuilder fbuilder = null;
        for (int i = 0; i < deltaSchema.getFields().length; i++) {
            StructField field = deltaSchema.getFields()[i];
            boolean nullable = field.isNullable();
            fbuilder = builder.field(field.getName());
            if (nullable){
                fbuilder = fbuilder.optional();
            } else {
                fbuilder = fbuilder.required();
            }
            if (field.getDataType() instanceof StringType) {
                fbuilder = fbuilder.type(SchemaType.STRING);
            } else if (field.getDataType() instanceof BooleanType) {
                fbuilder = fbuilder.type(SchemaType.BOOLEAN);
            } else if (field.getDataType() instanceof BinaryType) {
                fbuilder = fbuilder.type(SchemaType.BYTES);
            } else if (field.getDataType() instanceof DoubleType) {
                fbuilder = fbuilder.type(SchemaType.DOUBLE);
            } else if (field.getDataType() instanceof FloatType) {
                fbuilder = fbuilder.type(SchemaType.FLOAT);
            } else if (field.getDataType() instanceof IntegerType) {
                fbuilder = fbuilder.type(SchemaType.INT32);
            } else if (field.getDataType() instanceof LongType) {
                fbuilder = fbuilder.type(SchemaType.INT64);
            } else if (field.getDataType() instanceof ShortType) {
                fbuilder = fbuilder.type(SchemaType.INT16);
            } else if (field.getDataType() instanceof ByteType) {
                fbuilder = fbuilder.type(SchemaType.INT8);
            } else if (field.getDataType() instanceof NullType) {
                fbuilder = fbuilder.type(SchemaType.NONE);
            } else if (field.getDataType() instanceof DateType) {
                fbuilder = fbuilder.type(SchemaType.DATE);
            } else if (field.getDataType() instanceof TimestampType) {
                fbuilder = fbuilder.type(SchemaType.TIMESTAMP);
            } else if (field.getDataType() instanceof DecimalType) {
                fbuilder = fbuilder.type(SchemaType.DOUBLE);
            } else { // not support other data type
                fbuilder = fbuilder.type(SchemaType.STRING);
            }
        }
        if (fbuilder == null) {
            throw new IOException("filed is empty, can not covert to pulsar schema");
        }

        GenericSchema<GenericRecord> t = Schema.generic(builder.build(SchemaType.AVRO));
        log.info("try convert delta Schema to pulsar schema {} pulsar schema {}", deltaSchema, t.getSchemaInfo());
        return t;
    }

    private GenericRecord getGenericRecord(StructType deltaSchema, GenericSchema<GenericRecord> pulsarSchema,
                                           DeltaReader.RowRecordData rowRecordData) {
        GenericRecordBuilder builder;
        GenericRecord g;
        if (deltaSchema != null) {
            builder = s.newRecordBuilder();
            for (int i = 0; i < deltaSchema.getFields().length && i < rowRecordData.parquetSchema.size(); i++) {
                StructField field = deltaSchema.getFields()[i];
                Object value;
                if (field.getDataType() instanceof StringType) {
                    value = rowRecordData.simpleGroup.getString(i, 0);
                } else if (field.getDataType() instanceof BooleanType) {
                    value = rowRecordData.simpleGroup.getBoolean(i, 0);
                } else if (field.getDataType() instanceof BinaryType) {
                    value = rowRecordData.simpleGroup.getBinary(i, 0);
                } else if (field.getDataType() instanceof DoubleType) {
                    value = rowRecordData.simpleGroup.getDouble(i, 0);
                } else if (field.getDataType() instanceof FloatType) {
                    value = rowRecordData.simpleGroup.getFloat(i, 0);
                } else if (field.getDataType() instanceof IntegerType) {
                    value = rowRecordData.simpleGroup.getInteger(i, 0);
                } else if (field.getDataType() instanceof LongType) {
                    value = rowRecordData.simpleGroup.getLong(i, 0);
                } else if (field.getDataType() instanceof ShortType) {
                    value = rowRecordData.simpleGroup.getInteger(i, 0);
                } else if (field.getDataType() instanceof ByteType) {
                    value = rowRecordData.simpleGroup.getInteger(i, 0);
                } else if (field.getDataType() instanceof NullType) {
                    value = rowRecordData.simpleGroup.getInteger(i, 0);
                } else if (field.getDataType() instanceof DateType) {
                    value = rowRecordData.simpleGroup.getTimeNanos(i, 0);
                } else if (field.getDataType() instanceof TimestampType) {
                    value = rowRecordData.simpleGroup.getTimeNanos(i, 0);
                } else if (field.getDataType() instanceof DecimalType) {
                    value = rowRecordData.simpleGroup.getDouble(i, 0);
                } else { // not support other data type
                    value = rowRecordData.simpleGroup.getValueToString(i, 0);
                }
                builder.set(field.getName(), value);
            }
            g = builder.build();
            return g;
        } else if (pulsarSchema != null) {
            builder = pulsarSchema.newRecordBuilder();

            final org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
            parser.setValidateDefaults(false);
            org.apache.avro.Schema avroSchema =
                    parser.parse(new String(pulsarSchema.getSchemaInfo().getSchema(),
                            java.nio.charset.StandardCharsets.UTF_8));
            log.info("parquet Schema: {} schemaInfo: {}", rowRecordData.parquetSchema, pulsarSchema.getSchemaInfo());
            for (int i = 0; i < avroSchema.getFields().size() && i < rowRecordData.parquetSchema.size(); i++) {
                org.apache.avro.Schema.Field field = avroSchema.getFields().get(i);
                  Object value;
                  org.apache.avro.Schema.Type type = field.schema().getType();
                  if (type == org.apache.avro.Schema.Type.STRING) {
                      value = rowRecordData.simpleGroup.getString(i, 0);
                  } else if (type == org.apache.avro.Schema.Type.INT) {
                      value = rowRecordData.simpleGroup.getInteger(i, 0);
                  } else if (type == org.apache.avro.Schema.Type.LONG) {
                      value = rowRecordData.simpleGroup.getLong(i, 0);
                  } else if (type == org.apache.avro.Schema.Type.FLOAT) {
                      value = rowRecordData.simpleGroup.getFloat(i, 0);
                  } else if (type == org.apache.avro.Schema.Type.DOUBLE) {
                      value = rowRecordData.simpleGroup.getDouble(i, 0);
                  } else {
                      value = rowRecordData.simpleGroup.getValueToString(i, 0);
                  }
                  builder.set(field.name(), value);
            }
            g = builder.build();
            return g;
        } else {
            return null;
        }
    }

    @Override
    public Optional<String> getTopicName() {
        return Optional.empty();
    }

    @Override
    public Optional<String> getKey() {
        return Optional.of(partitionValue);
    }

    @Override
    public Schema<GenericRecord> getSchema() {
        return s;
    }

    @Override
    public GenericRecord getValue() {
        return value;
    }

    @Override
    public Optional<Long> getEventTime() {
        try {
            Long s = Long.parseLong(properties.get(TS_FIELD));
            return Optional.of(s);
        } catch (NumberFormatException e) {
            return Optional.of(0L);
        }
    }

    @Override
    public Optional<String> getPartitionId() {
        return Optional.empty();
    }

    @Override
    public Optional<Integer> getPartitionIndex() {
      return Optional.empty();
    }

    @Override
    public Optional<Long> getRecordSequence() {

        return Optional.of(sequence);
    }

    @Override
    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    public void ack() {
        DeltaCheckpoint checkpoint;
        if (this.rowRecordData.nextCursor.isFullSnapShot) {
            checkpoint = new DeltaCheckpoint(DeltaCheckpoint.StateType.FULL_COPY,
                    this.rowRecordData.nextCursor.version);
            checkpoint.setMetadataChangeFileIndex(this.rowRecordData.nextCursor.changeIndex);
            checkpoint.setRowNum(this.rowRecordData.nextCursor.rowNum);
            checkpoint.setSeqCount(sequence);
        } else {
            checkpoint = new DeltaCheckpoint(DeltaCheckpoint.StateType.INCREMENTAL_COPY,
                    this.rowRecordData.nextCursor.version);
            checkpoint.setMetadataChangeFileIndex(this.rowRecordData.nextCursor.changeIndex);
            checkpoint.setRowNum(this.rowRecordData.nextCursor.rowNum);
            checkpoint.setSeqCount(sequence);
        }
        saveCheckpointMap.put((int) partition, checkpoint);
    }

    @Override
    public void fail() {
        log.info("send message partition {} sequence {} failed", partition, sequence);
        processingException.incrementAndGet();
    }

    @Override
    public Optional<String> getDestinationTopic() {
        return Optional.of(this.topic);
    }

    @Override
    public Optional<Message<GenericRecord>> getMessage() {
        return Optional.empty();
    }
}
