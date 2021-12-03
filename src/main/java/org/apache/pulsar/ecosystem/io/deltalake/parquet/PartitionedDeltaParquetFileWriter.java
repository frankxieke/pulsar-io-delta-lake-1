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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.pulsar.ecosystem.io.deltalake.parquet.DeltaParquetFileWriterInterface.FileStat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The partitioned parquet file write.
 */
public class PartitionedDeltaParquetFileWriter implements DeltaParquetFileWriterInterface {
    private static final Logger log = LoggerFactory.getLogger(PartitionedDeltaParquetFileWriter.class);
    public final Map<String, DeltaParquetFileWriter> writerMap = new ConcurrentHashMap<>();
    public String tablePath;
    public Schema avroSchema;
    public List<String> partitionColumns;
    public Configuration conf;

    public PartitionedDeltaParquetFileWriter(Configuration conf, String tablePath,
                                             List<String> partitionColumns, Schema avroSchema) {
        this.partitionColumns = partitionColumns;
        this.avroSchema = avroSchema;
        this.tablePath = tablePath;
        this.conf = conf;
    }

    @Override
    public void writeToParquetFile(List<GenericRecord> recordList) throws IOException {
       for (GenericRecord record: recordList) {
            String partitionValue = DeltaParquetFileWriter.getPartitionValuePath(record, partitionColumns);
            DeltaParquetFileWriter writer = writerMap.get(partitionValue);
            if (writer == null) {
                writer = new DeltaParquetFileWriter(conf, tablePath, avroSchema,
                                DeltaParquetFileWriter.getPartitionValues(record, partitionColumns));
                writerMap.put(partitionValue, writer);
            }
            List<GenericRecord> oneList = new ArrayList<>();
            oneList.add(record);
            writer.writeToParquetFile(oneList);
        }
    }

    @Override
    public long getMinFileSize() {
        Long minSize = -1L;
        for (Map.Entry<String, DeltaParquetFileWriter> entry: writerMap.entrySet()) {
            DeltaParquetFileWriter writer = entry.getValue();
            if (minSize == -1 || writer.getMinFileSize() < minSize) {
                minSize = writer.getMinFileSize();
            }
        }
        return minSize;
    }

    @Override
    public List<FileStat> closeAndFlush() throws IOException {
        List<FileStat> fileStatList = new ArrayList<>();
        for (Map.Entry<String, DeltaParquetFileWriter> entry: writerMap.entrySet()) {
            DeltaParquetFileWriter writer = entry.getValue();
            List<FileStat> tmp = writer.closeAndFlush();
            fileStatList.addAll(tmp);
        }
        return fileStatList;
    }

    @Override
    public long getLastRollTimeStamp() {
        for (Map.Entry<String, DeltaParquetFileWriter> entry: writerMap.entrySet()) {
            DeltaParquetFileWriter writer = entry.getValue();
            return writer.getLastRollTimeStamp();
        }
        return 0;
    }

    @Override
    public void rollFile(Schema newSchema) throws IOException {
        for (Map.Entry<String, DeltaParquetFileWriter> entry: writerMap.entrySet()) {
            DeltaParquetFileWriter writer = entry.getValue();
            writer.rollFile(newSchema);
        }
    }
}
