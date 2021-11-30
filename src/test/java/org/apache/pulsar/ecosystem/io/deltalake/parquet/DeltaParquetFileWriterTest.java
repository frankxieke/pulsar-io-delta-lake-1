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
//import java.util.ArrayList;
//import java.util.List;
import junit.framework.TestCase;
//import org.apache.avro.Schema;
//import org.apache.avro.generic.GenericData;
//import org.apache.pulsar.ecosystem.io.deltalake.sink.DeltaWriterThread;

/**
 * The parquet file read util.
 */
public class DeltaParquetFileWriterTest extends TestCase {

    public void testWriteToParquetFile() throws IOException {
//        Schema.Parser parser = new Schema.Parser();
//        String json = "{\"type\":\"record\",\"name\":\"Record\",\"namespace\":\"org.example.producer\","
//                + "\"fields\":[{\"name\":\"c1\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"c2\","
//                + "\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"c3\",\"type\":[\"null\",\"string\"],"
//                + "\"default\":null},{\"name\":\"c4\",\"type\":[\"null\",\"string\"],\"default\":null}]}";
//        Schema schema = parser.parse(json);
//        DeltaParquetFileWriter writer = new DeltaParquetFileWriter("/Users/kexie/data811235.parquet", schema);
//        writer.open();
//        List<GenericData.Record> recordList = new ArrayList<GenericData.Record>();
//        GenericData.Record record = new GenericData.Record(schema);
//        record.put("c1", 1);
//        record.put("c2", 23);
//        record.put("c3", "hello world");
//        record.put("c4", "test");
//        record =  DeltaWriterThread.convertToAvroGenericData(schema) ;
//        recordList.add(record);
//        writer.writeToParquetFile(recordList);
//        writer.close();
    }
}