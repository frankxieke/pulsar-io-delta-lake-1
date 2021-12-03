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
import io.delta.standalone.VersionLog;
import io.delta.standalone.actions.Action;
import io.delta.standalone.actions.AddFile;
import io.delta.standalone.actions.Format;
import io.delta.standalone.actions.Metadata;
import io.delta.standalone.actions.SetTransaction;
import io.delta.standalone.types.StructType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import junit.framework.TestCase;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.pulsar.ecosystem.io.deltalake.schema.SchemaConvert;

/**
 * The delta writer test.
 */
public class DeltaWriterTest extends TestCase {

    public void testRead() {
        String pulsarSchema = "{\"type\":\"record\",\"name\":\"Record\",\"namespace\":\"org.example.producer\","
                + "\"fields\":[{\"name\":\"c1\",\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"c2\","
                + "\"type\":[\"null\",\"long\"],\"default\":null},{\"name\":\"c3\",\"type\":[\"null\",\"string\"],"
                + "\"default\":null},{\"name\":\"record\",\"type\":[\"null\",{\"type\":\"record\","
                + "\"name\":\"InnerRecord\",\"namespace\":\"org.example.producer$Record\","
                + "\"fields\":[{\"name\":\"address\",\"type\":[\"null\",\"string\"],\"default\":null},"
                + "{\"name\":\"age\",\"type\":\"int\"}]}],\"default\":null},{\"name\":\"recordsList\","
                + "\"type\":[\"null\",{\"type\":\"array\",\"items\":\"org.example.producer$Record.InnerRecord\","
                + "\"java-class\":\"java.util.List\"}],\"default\":null},{\"name\":\"mapField\",\"type\":[\"null\","
                + "{\"type\":\"map\",\"values\":\"long\"}],\"default\":null},{\"name\":\"mapRecord\","
                + "\"type\":[\"null\",{\"type\":\"map\",\"values\":\"org.example.producer$Record.InnerRecord\"}],"
                + "\"default\":null},{\"name\":\"mapListRecord\",\"type\":[\"null\",{\"type\":\"map\","
                + "\"values\":{\"type\":\"array\",\"items\":\"org.example.producer$Record.InnerRecord\","
                + "\"java-class\":\"java.util.List\"}}],\"default\":null}]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(pulsarSchema);
        System.out.println("schema " + schema);
        StructType structType =  SchemaConvert.convertAvroSchemaToDeltaSchema(schema);
        System.out.println(structType.toPrettyJson());
        DeltaLog logW = DeltaLog.forTable(new Configuration(), "/Users/kexie/data.parquet0/");


        OptimisticTransaction optimisticTransaction = logW.startTransaction();
        System.out.println("currentV:" + optimisticTransaction.txnVersion("appId3"));
        Metadata me = optimisticTransaction.metadata();
        optimisticTransaction.readWholeTable();
        System.out.println("tranV:" + optimisticTransaction.txnVersion("appId1"));
        System.out.println(me.getSchema().getTreeString());
        String id  = UUID.randomUUID().toString();
        String name = "hello test";
        String description = "first metadata action";
        Format format = new Format();
        List<String> partitionCols = new ArrayList<String>();
        Map<String, String> configuration = new HashMap<String, String>();
        Optional<Long> creatTime = Optional.of(System.currentTimeMillis());

        Metadata newMe = new Metadata(id, name, description, format,
                partitionCols, configuration, creatTime, structType);

        SetTransaction setTransaction = new SetTransaction("appId1", 1, Optional.of(System.currentTimeMillis()));
        AddFile add = new AddFile("part-0000-4958853b-caa7-491a-b9f6-767b1af20c25-c000.snappy.parquet",
                new HashMap<String, String>(), 51310084, System.currentTimeMillis(), true, "\"{}\"", null);

        List<Action> filesToCommit =
                Arrays.asList(setTransaction, newMe, add);

        CommitResult result = optimisticTransaction.commit(filesToCommit,
                new Operation(Operation.Name.CREATE_TABLE), "Pulsar-Sink-Connector-version_2.9.0");
        System.out.println(result.getVersion());
        Iterator<VersionLog> vlogs = logW.getChanges(0, true);
        while (vlogs.hasNext()) {
            VersionLog v = vlogs.next();
            System.out.println("action:" + v.getActions().size() + " version:" + v.getVersion());
            List<Action> actions = v.getActions();
            for (int i = 0; i < actions.size(); i++) {
                System.out.println("i:" + i + " action:" + actions.get(i));
            }
        }
    }
}