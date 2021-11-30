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

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.pulsar.ecosystem.io.deltalake.parquet.DeltaParquetFileWriter;
import org.apache.pulsar.functions.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The delta reader thread class for {@link DeltaLakeConnectorSink}.
 */
public class DeltaWriterThread extends Thread {
    private static final Logger log = LoggerFactory.getLogger(DeltaWriterThread.class);
    private final DeltaLakeConnectorSink sink;
    public boolean stopped;
    public DeltaWriterThread(DeltaLakeConnectorSink sink) {
        this.sink = sink;
        stopped = false;
    }
    public static GenericRecord convertToAvroGenericData(
            Record<org.apache.pulsar.client.api.schema.GenericRecord> s) {

        log.info("class {}", s.getValue().getNativeObject().getClass());
        GenericRecord r =
                (GenericRecord) s.getValue().getNativeObject();
        return r;
    }

    public static Schema.Field convertOneField(org.apache.avro.Schema.Field f) {
        org.apache.avro.Schema.Field newField = null;
            switch(f.schema().getType()) {
                case UNION:
                    List<org.apache.avro.Schema> types = f.schema().getTypes();
                    List<org.apache.avro.Schema> validTypes = new ArrayList<>();
                    types.forEach(t->{
                        // System.out.println("type:"+t);
                        if (t.getType() != org.apache.avro.Schema.Type.NULL) {
                            validTypes.add(t);
                        }
                    });
                    if (validTypes.size() == 1) {
                        org.apache.avro.Schema.Field tmp =
                                new org.apache.avro.Schema.Field(f.name(), validTypes.get(0), f.doc(), null);
                        newField = convertOneField(tmp);
                        log.info("add valid type: " + validTypes);
                    } else {
                        log.error("not support this kind of union types {}", types);
                    }
                    break;
                case RECORD:
                    List<org.apache.avro.Schema.Field> fields = f.schema().getFields();
                    List<org.apache.avro.Schema.Field> newFields = new ArrayList<>();
                    for (int i = 0; i < fields.size(); i++) {
                        org.apache.avro.Schema.Field tmp = convertOneField(fields.get(i));
                        newFields.add(tmp);
                    }
                    org.apache.avro.Schema newSchema = org.apache.avro.Schema.createRecord(
                            f.schema().getName(), f.schema().getDoc(), f.schema().getNamespace(), f.schema().isError());
                    newSchema.setFields(newFields);
                    log.info("add new record: " + newSchema);
                    newField = new org.apache.avro.Schema.Field(f.name(), newSchema, f.doc(), null);
                    break;
                default:
                    newField = new org.apache.avro.Schema.Field(f.name(), f.schema(), f.doc(), null);
                    log.info("valid type: " + f.schema().getType());
            }
        return newField;
    }
    public static GenericRecord convertJsontoAvroGenericRecord(
            Record<org.apache.pulsar.client.api.schema.GenericRecord> s,
            Schema schema) throws IOException {

        List<org.apache.avro.Schema.Field> fields = schema.getFields();
        List<org.apache.avro.Schema.Field> newFields = new ArrayList<>();
        fields.forEach(f->{
            newFields.add(convertOneField(f));
//            switch(f.schema().getType()) {
//                case UNION:
//                    List<org.apache.avro.Schema> types = f.schema().getTypes();
//                    List<org.apache.avro.Schema> validTypes = new ArrayList<>();
//                    types.forEach(t->{
//                        // System.out.println("type:"+t);
//                        if (t.getType() != org.apache.avro.Schema.Type.NULL) {
//                            validTypes.add(t);
//                        }
//                    });
//                    if (validTypes.size() == 1) {
//                        org.apache.avro.Schema.Field
//                                newField = new org.apache.avro.Schema.Field(
//                                f.name(), validTypes.get(0), f.doc(), null);
//                        newFields.add(newField);
//                    }
//                    System.out.println("add valid type: " + validTypes);
//                    break;
//                default:
//                    org.apache.avro.Schema.Field
//                            newField = new org.apache.avro.Schema.Field(f.name(), f.schema(), f.doc(), null);
//                    newFields.add(newField);
//                    System.out.println("valid type: " + f.schema().getType());
//            }
        });

        org.apache.avro.Schema newSchema = org.apache.avro.Schema.createRecord(
                schema.getName(), schema.getDoc(), schema.getNamespace(), schema.isError());

        newSchema.setFields(newFields);
        DatumReader<GenericRecord>
                reader = new GenericDatumReader<GenericRecord>(newSchema, newSchema);
        DecoderFactory decoderFactory = new DecoderFactory();
        String tmp = ((ObjectNode) (s.getValue()).getNativeObject()).toString();
        Decoder decoder = decoderFactory.jsonDecoder(newSchema, tmp);
        org.apache.avro.generic.GenericRecord r = reader.read(null, decoder);
        return r;
    }
    public void run() {
        DeltaParquetFileWriter writer = null;
        int cnt = 0;
        while (!stopped) {
            writer = null;
            try {
                log.info("read thread proccessing");
                DeltaSinkRecord deltaRecord = this.sink.queue.take();
                log.info("get original record {} {} {} {} {}", deltaRecord.record.getTopicName(),
                        deltaRecord.record.getSchema(),
                        deltaRecord.record.getValue().getSchemaType(),
                        deltaRecord.record.getMessage().get().getMessageId(),
                        deltaRecord.record.getSchema().getSchemaInfo().toString());
                org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
                Schema schema = parser.parse(
                        deltaRecord.record.getSchema().getSchemaInfo().getSchemaDefinition());
                switch (deltaRecord.record.getValue().getSchemaType()) {
                    case AVRO:
                        log.info("get avro generic before record {} ", deltaRecord.record.getValue().toString());
                        GenericRecord r = null;
                        r = convertToAvroGenericData(deltaRecord.record);
                        if (writer == null) {
                            log.info("get avro1 generic before record {} ", r.getSchema());

                            writer = new DeltaParquetFileWriter("/Users/kexie/data.parquet" + cnt++, schema);
                            writer.open();
                        }
                        log.info("get avro generic after record {} ", r.toString());
                        List<GenericRecord> list = new LinkedList<>();
                        list.add(r);
                        writer.writeToParquetFile(list);
                        log.info("begin to close()");
                        writer.close();
                        writer = null;
                        log.info("get avro 1111generic record {} ", r.toString());
                        break;
                    case JSON:
//                        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
//                        DecoderFactory decoderFactory = new DecoderFactory();
//                        Decoder decoder = decoderFactory.jsonDecoder(schema,
//                                ((ObjectNode) deltaRecord.record.getValue().getNativeObject()).toString());
//                        r = reader.read(null, decoder);
                        r = convertJsontoAvroGenericRecord(deltaRecord.record, schema);
                        if (writer == null) {
                            writer = new DeltaParquetFileWriter("/Users/kexie/data.parquet" + cnt++, schema);
                            writer.open();
                        }
                        list = new LinkedList<>();
                        list.add(r);
                        writer.writeToParquetFile(list);
                        log.info("begin to closeJson()");
                        writer.close();
                        writer = null;
                        log.info("get json 222 generic record {} ", r.toString());
                        break;
                    default:
                        log.info("get other kind of generic record {}",
                        deltaRecord.record.getValue().getSchemaType());
                }
            } catch (InterruptedException | IOException | UnsupportedOperationException e) {
                log.error("prccess record failed, ", e);
            } catch (Exception e2) {
                log.error("process other kind of exception, ", e2);
            } finally {
            }
        }
    }
    public void close() {
        this.stopped = true;
    }
}
