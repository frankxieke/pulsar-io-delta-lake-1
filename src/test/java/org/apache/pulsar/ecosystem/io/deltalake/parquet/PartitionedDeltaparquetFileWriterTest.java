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

import java.util.ArrayList;
import java.util.Map;
import junit.framework.TestCase;
import org.apache.avro.generic.GenericData;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The PartitionedDeltaparquetFileWriterTest test PartitionedDeltaparquetFileWriter.
 */
public class PartitionedDeltaparquetFileWriterTest extends TestCase {
    private static final Logger log = LoggerFactory.getLogger(PartitionedDeltaparquetFileWriterTest.class);

    public void testGetPartitionValue() {
        org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        String schemaInfo = "{\n"
                + "  \"type\":   \"record\",\n"
                + "  \"name\":   \"testFile\",\n"
                + "  \"doc\":    \"test records\",\n"
                + "  \"fields\":\n"
                + "    [{\"name\":   \"num\",\"type\":    \"int\"},\n"
                + "    {\"name\":    \"date\",\"type\":  \"string\"},\n"
                + "    {\"name\":    \"province\",\"type\":  \"string\"},\n"
                + "    {\"name\":    \"country\",\"type\":   \"string\"}\n"
                + "  ]\n"
                + "}";
        org.apache.avro.Schema schema = parser.parse(schemaInfo);
        GenericData.Record record = new GenericData.Record(schema);
        record.put("date", "2021-12-01");
        record.put("province", "shandong");
        record.put("country", "China");
        record.put("num", 1000);
        ArrayList<String> partitionColumns = new ArrayList<String>();
        partitionColumns.add("date");
        partitionColumns.add("province");
        partitionColumns.add("country");
        String path = DeltaParquetFileWriter.getPartitionValuePath(record, partitionColumns);
        Assert.assertTrue(path.equals("country=China/date=2021-12-01/province=shandong"));
        Map<String, String> partitionValues = DeltaParquetFileWriter.getPartitionValues(record, partitionColumns);
        Assert.assertTrue(partitionValues.get("province").equals("shandong")
                && partitionValues.get("date").equals("2021-12-01")
                && partitionValues.get("country").equals("China"));
        String path2 = DeltaParquetFileWriter.getPartitionValuePath(partitionValues);
        Assert.assertTrue(path.equals(path2));
    }
}