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

package org.apache.pulsar.ecosystem.io.deltalake.parquet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.pulsar.ecosystem.io.deltalake.parquet.DeltaParquetFileWriterInterface.FileStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The parquet file write util.
 */
public class DeltaParquetFileWriter implements DeltaParquetFileWriterInterface{
    private static final Logger log = LoggerFactory.getLogger(DeltaParquetFileWriter.class);
    public String tablePath;
    public Schema avroSchema;
    public ParquetWriter<GenericRecord> writer;
    private String partitionColumnPath;
    private Map<String, String> partitionValues;
    private Configuration conf;
    private Long lastRollFileTimeStamp;

    public String getCurrentFileFullPath() {
        return currentFileFullPath;
    }

    private String currentFileFullPath;
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    public static Map<String, String> getPartitionValues(GenericRecord genericRecord, List<String> partitionColumns) {
        Map<String, String> partitionValues = new ConcurrentHashMap<>();
        if (partitionColumns.size() == 0 || partitionColumns == null) {
            return partitionValues;
        }
        for (int i = 0; i < partitionColumns.size(); i++) {
            String partitionColumn = partitionColumns.get(i);
            org.apache.avro.Schema.Field field = genericRecord.getSchema().getField(partitionColumn);
            if (field == null) {
                continue;
            }
            partitionValues.put(partitionColumn, String.valueOf(genericRecord.get(field.name())));
        }
        return partitionValues;
    }

    public static String getPartitionValuePath(Map<String, String> partitionValues) {
        if (partitionValues == null || partitionValues.size() == 0) {
            return "";
        }
        Map<String, String> map = new TreeMap<String, String>();
        for (Map.Entry<String, String> entry : partitionValues.entrySet()) {
            map.put(entry.getKey(), entry.getValue());
        }

        StringBuilder pathBuilder = new StringBuilder();
        Set<String> keySet = map.keySet();
        Iterator<String> iter = keySet.iterator();
        while (iter.hasNext()) {
            String key = iter.next();
            pathBuilder.append(key);
            pathBuilder.append("=");
            pathBuilder.append(map.get(key));
            if (iter.hasNext()) {
                pathBuilder.append("/");
            }
        }
        return pathBuilder.toString();
    }

    public static String getPartitionValuePath(GenericRecord genericRecord, List<String> partitionColumns) {
        if (partitionColumns.size() == 0 || partitionColumns == null) {
            return "";
        }
        Collections.sort(partitionColumns);
        StringBuilder pathBuilder = new StringBuilder();
        for (int i = 0; i < partitionColumns.size(); i++) {
            String partitionColumn = partitionColumns.get(i);
            org.apache.avro.Schema.Field field = genericRecord.getSchema().getField(partitionColumn);
            if (field == null) {
                return "";
            }
            pathBuilder.append(field.name());
            pathBuilder.append("=");
            pathBuilder.append(genericRecord.get(field.name()));
            if (i + 1 < partitionColumns.size()) {
                pathBuilder.append("/");
            }
        }
        return pathBuilder.toString();
    }

    private void getNextFilePath() {
        StringBuilder sb = new StringBuilder();
        String filePath;
        if (this.partitionColumnPath != null) {
            filePath = sb.append(tablePath).append("/").append(this.partitionColumnPath).append("/part-0000-")
                    .append(UUID.randomUUID().toString())
                    .append("-c000.snappy.parquet").toString();
        } else {
            filePath = sb.append(tablePath).append("/part-0000-")
                    .append(UUID.randomUUID().toString())
                    .append("-c000.snappy.parquet").toString();
        }

        this.currentFileFullPath = filePath;
    }

    private void close() throws IOException {
        if (this.isClosed.get()) {
            return;
        }
        if (writer != null) {
            try {
                log.info("begin to close internal writer");
                this.writer.close();
            } catch (IOException e) {
                log.error("close parquetWriter failed for {} ", currentFileFullPath, e);
                throw e;
            } finally {
                this.isClosed.set(true);
                this.writer = null;
                this.currentFileFullPath = "";
            }
        }

    }

    public DeltaParquetFileWriter(Configuration conf, String tablePath,
                                  Schema avroSchema) {
        this.tablePath = tablePath;
        this.avroSchema = avroSchema;
        this.lastRollFileTimeStamp = System.currentTimeMillis();
        this.partitionColumnPath = null;
        this.currentFileFullPath = "";
        this.conf = conf;
    }
    public DeltaParquetFileWriter(Configuration conf, String tablePath,
                                  Schema avroSchema, Map<String, String> partitionValues) {
        this.tablePath = tablePath;
        this.avroSchema = avroSchema;
        this.lastRollFileTimeStamp = System.currentTimeMillis();
        this.partitionColumnPath = getPartitionValuePath(partitionValues);
        this.partitionValues = partitionValues;
        this.currentFileFullPath = "";
        this.conf = conf;
    }

    private void openNewFile() throws IOException {
        writer = AvroParquetWriter.
                <GenericRecord>builder(new Path(currentFileFullPath))
                .withRowGroupSize(org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withPageSize(org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE)
                .withSchema(avroSchema)
                .withConf(conf)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withValidation(false)
                .withDictionaryEncoding(false)
                .build();
        this.isClosed.set(false);
        log.info("open {} parquet succeed {}", currentFileFullPath, writer);
    }


    @Override
    public void writeToParquetFile(List<GenericRecord> recordList) throws IOException {
        this.lock.lock();
        try {
            if (this.isClosed.get() || this.currentFileFullPath.equals("")) {
                // to open new file
                getNextFilePath();
                openNewFile();
            }

            log.info("begin to write avro record, filePath {} listSize {} filesize {}",
                    this.currentFileFullPath, recordList.size(), getMinFileSize());
            for (GenericRecord record : recordList) {
                writer.write(record);
            }
        } finally {
            this.lock.unlock();
        }
    }

    @Override
    public long getMinFileSize() {
        if (writer != null) {
            return writer.getDataSize();
        }
        return -1L;
    }

    @Override
    public List<FileStat> closeAndFlush() throws IOException {
        List<FileStat> fileStatList = new ArrayList<>();
        this.lock.lock();
        try {
            if (this.isClosed.get()) {
                return fileStatList;
            }
            String filePath = currentFileFullPath.substring(currentFileFullPath.lastIndexOf('/') + 1);
            FileStat fileStat = new FileStat();
            fileStat.filePath = filePath;
            fileStat.fileSize = getMinFileSize();
            fileStat.partitionValues = new HashMap<>();
            fileStatList.add(fileStat);
            close();
        } finally {
            this.lock.unlock();
        }
        return fileStatList;
    }

    @Override
    public long getLastRollTimeStamp() {
        return this.lastRollFileTimeStamp;
    }

    @Override
    public void rollFile(Schema newSchema) throws IOException {
        this.lock.lock();
        try {
            close();
        } finally {
            this.lastRollFileTimeStamp = System.currentTimeMillis();
            this.avroSchema = newSchema;
            this.lock.unlock();
        }
    }
}
