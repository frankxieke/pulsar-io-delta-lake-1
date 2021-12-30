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
package org.apache.pulsar.ecosystem.io.deltalake.sink;


import io.delta.standalone.CommitResult;
import io.delta.standalone.DeltaLog;
import io.delta.standalone.Operation;
import io.delta.standalone.OptimisticTransaction;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Format;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.actions.SetTransaction;
import io.delta.standalone.types.StructType;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.pulsar.ecosystem.io.deltalake.parquet.DeltaParquetFileWriter;
import org.apache.pulsar.ecosystem.io.deltalake.parquet.DeltaParquetFileWriterInterface;
import org.apache.pulsar.ecosystem.io.deltalake.parquet.PartitionedDeltaParquetFileWriter;
import org.apache.pulsar.ecosystem.io.deltalake.schema.SchemaConvert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The delta writer for {@link DeltaWriterThread}.
 */
public class DeltaWriter {
    private static final Logger log = LoggerFactory.getLogger(DeltaWriter.class);
    public DeltaLakeSinkConnectorConfig config;
    public Configuration conf;
    public String appId; //used by delta commit
    private DeltaLog deltaLog;
    private org.apache.pulsar.ecosystem.io.deltalake.parquet.DeltaParquetFileWriterInterface writer;
    public Schema pulsarAvroSchema;
    public boolean firstCommit;

    public DeltaWriter(DeltaLakeSinkConnectorConfig config, String appId,
                       Schema pulsarAvroSchema) throws Exception {
        this.config = config;
        this.appId = appId;
        this.pulsarAvroSchema = pulsarAvroSchema;
        firstCommit = true;
        open();
    }

    private void open() throws Exception {
        conf = new Configuration();
        if (config.fileSystemType.equals(config.S3Type)) {
            conf.set("fs.s3a.access.key", config.s3aAccesskey);
            conf.set("fs.s3a.secret.key", config.s3aSecretKey);
            conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        }
        deltaLog = DeltaLog.forTable(conf, config.tablePath);
        if (config.partitionColumns != null && config.partitionColumns.size() > 0) {
            writer = new PartitionedDeltaParquetFileWriter(conf, config.tablePath,
                    config.partitionColumns, pulsarAvroSchema);
        } else {
            writer = new DeltaParquetFileWriter(conf, config.tablePath, pulsarAvroSchema);
        }
    }

    public void writeAvroRecord(GenericRecord record, Schema schema) throws IOException {
        if (firstCommit) {
            createTable(schema);
            firstCommit = false;
        }
        if (checkSchemaChange(schema)) {
            log.info("pulsar schema change to {}", schema);
            List<DeltaParquetFileWriterInterface.FileStat> fileStatList = writer.closeAndFlush();
            addParquetFiles(fileStatList);
            commitMetadata(schema);
        }
        List<GenericRecord> oneList = new ArrayList<>();
        oneList.add(record);
        writer.writeToParquetFile(oneList);
        if (writer.getMinFileSize() > config.maxParquetFileBytesSize
            || writer.getLastRollTimeStamp() + config.maxWaitFlushSecondsInterval * 1000 < System.currentTimeMillis()) {
           List<DeltaParquetFileWriterInterface.FileStat> fileStatList = writer.closeAndFlush();
           addParquetFiles(fileStatList);
        }
    }

    public boolean checkSchemaChange(Schema newAvroSchema) {
        String newSchema = newAvroSchema.toString();
        String oldSchema = pulsarAvroSchema.toString();
        if (!oldSchema.equals(newSchema)) {
            return true;
        }
        return false;
    }

    public void createTable(Schema pulsarAvroSchema) {
        OptimisticTransaction optimisticTransaction = deltaLog.startTransaction();
        String id  = UUID.randomUUID().toString();
        String name = "metadata";
        String description = "metadata change";
        Format format = new Format();
        List<String> partitionCols = new ArrayList<String>();
        Map<String, String> configuration = new HashMap<String, String>();
        Optional<Long> creatTime = Optional.of(System.currentTimeMillis());
        StructType structType = SchemaConvert.convertAvroSchemaToDeltaSchema(pulsarAvroSchema);
        Metadata newMe = new Metadata(id, name, description, format,
                partitionCols, configuration, creatTime, structType);
        long version = optimisticTransaction.txnVersion(appId);
        SetTransaction setTransaction = new SetTransaction(appId, version + 1, Optional.of(System.currentTimeMillis()));
        List<Action> filesToCommit = new ArrayList<>();
        filesToCommit.add(setTransaction);
        filesToCommit.add(newMe);
        CommitResult result = optimisticTransaction.commit(filesToCommit, new Operation(Operation.Name.CREATE_TABLE),
                "Pulsar-Sink-Connector-version_2.9.0");
        log.info("create table delta schema succeed to {}", newMe);
    }
    public void commitMetadata(Schema pulsarAvroSchema) {
        OptimisticTransaction optimisticTransaction = deltaLog.startTransaction();
        String id  = UUID.randomUUID().toString();
        String name = "metadata";
        String description = "meetadata change";
        Format format = new Format();
        List<String> partitionCols = new ArrayList<String>();
        Map<String, String> configuration = new HashMap<String, String>();
        Optional<Long> creatTime = Optional.of(System.currentTimeMillis());
        StructType structType = SchemaConvert.convertAvroSchemaToDeltaSchema(pulsarAvroSchema);
        Metadata newMe = new Metadata(id, name, description, format,
                partitionCols, configuration, creatTime, structType);
        optimisticTransaction.updateMetadata(newMe);
        log.info("update delta schema succeed to {}", newMe);
    }

    public void addParquetFiles(List<DeltaParquetFileWriterInterface.FileStat> parquetFiles) {
        if (parquetFiles.size() <= 0) {
            return;
        }
        OptimisticTransaction optimisticTransaction = deltaLog.startTransaction();
        long version = optimisticTransaction.txnVersion(appId);
        SetTransaction setTransaction = new SetTransaction(appId, version + 1, Optional.of(System.currentTimeMillis()));
        List<Action> filesToCommit = new ArrayList<>();
        filesToCommit.add(setTransaction);
        for (int i = 0; i < parquetFiles.size(); i++) {
            log.info("add filePath {} partitionValues {} fileSize {}",
                    parquetFiles.get(i).filePath
                    , parquetFiles.get(i).partitionValues
                    , parquetFiles.get(i).fileSize);
            AddFile add = new AddFile(parquetFiles.get(i).filePath,
                    parquetFiles.get(i).partitionValues, parquetFiles.get(i).fileSize,
                    System.currentTimeMillis(), true, "\"{}\"", null);
            filesToCommit.add(add);
        }
        log.info("add parquet files {}", filesToCommit);
        CommitResult result = optimisticTransaction.commit(filesToCommit, new Operation(Operation.Name.WRITE),
                "Pulsar-Sink-Connector-version_2.9.0");
        log.debug("commit to delta table succeed for {} files, commit version {}",
                parquetFiles.size(), result.getVersion());
    }
}
