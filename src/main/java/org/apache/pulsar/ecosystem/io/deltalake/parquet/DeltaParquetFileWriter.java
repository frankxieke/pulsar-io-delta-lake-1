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
import java.io.InputStream;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The parquet file write util.
 */
public class DeltaParquetFileWriter {
    private static final Logger log = LoggerFactory.getLogger(DeltaParquetFileWriter.class);

    public String path;
    public Schema avroSchema;
    public ParquetWriter<GenericRecord> writer;


    public void close() {
        if (writer != null) {
            try {
                log.info("begin to close internal writer");
                this.writer.close();
            } catch (IOException e) {
                log.error("close parquetWriter failed for {} ", path, e);
            }
        }
    }

    public DeltaParquetFileWriter(String filePath, Schema avroSchema) {
        this.path = filePath;
        this.avroSchema = avroSchema;
    }

    public void open() throws IOException {
        writer = AvroParquetWriter.
                <GenericRecord>builder(new Path(path))
                .withRowGroupSize(org.apache.parquet.hadoop.ParquetWriter.DEFAULT_BLOCK_SIZE)
                .withPageSize(org.apache.parquet.hadoop.ParquetWriter.DEFAULT_PAGE_SIZE)
                .withSchema(avroSchema)
                .withConf(new Configuration())
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withValidation(false)
                .withDictionaryEncoding(false)
                .build();
        log.info("open {} parquet succeed {}", path, writer);
    }

    public long getFileSize() {
        if (writer != null) {
            return writer.getDataSize();
        }
        return 0L;
    }

    // Method to parse the schema
    private static Schema parseSchema() {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = null;
        try {
            InputStream in = ClassLoader.getSystemResourceAsStream("schema.avsc");
            System.out.println(in.toString());
            // Path to schema file
            schema = parser.parse(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return schema;
    }

    public void writeToParquetFile(
            List<GenericRecord> recordList) throws IOException {
        log.info("begin to write avro record, listSize {} writer {}", recordList.size(), writer);
        for (GenericRecord record : recordList) {
            writer.write(record);
        }
        log.info("begin to close parquet file");
        close();
        log.info("end to write avro record");
    }

}
