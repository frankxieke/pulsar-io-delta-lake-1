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

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.pulsar.ecosystem.io.deltalake.schema.SchemaConvert;
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
        GenericRecord r = (GenericRecord) s.getValue().getNativeObject();
        return r;
    }


    public static GenericRecord convertJsontoAvroGenericRecord(
            Record<org.apache.pulsar.client.api.schema.GenericRecord> s,
            Schema schema) throws IOException {

        Schema newSchema = SchemaConvert.convertPulsarAvroSchemaToNonNullSchema(schema);
        log.debug("newSchema after convert {}", newSchema);
        DatumReader<GenericRecord>
                reader = new GenericDatumReader<GenericRecord>(newSchema, newSchema);
        DecoderFactory decoderFactory = new DecoderFactory();
        String tmp = ((ObjectNode) (s.getValue()).getNativeObject()).toString();
        Decoder decoder = decoderFactory.jsonDecoder(newSchema, tmp);
        org.apache.avro.generic.GenericRecord r = reader.read(null, decoder);
        return r;
    }
    public void run() {
        DeltaWriter writer = null;
        while (!stopped) {
            try {
                DeltaSinkRecord deltaRecord = this.sink.queue.take();
                org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
                Schema schema = parser.parse(
                        deltaRecord.record.getSchema().getSchemaInfo().getSchemaDefinition());
                switch (deltaRecord.record.getValue().getSchemaType()) {
                    case AVRO:
                        GenericRecord r = null;
                        r = convertToAvroGenericData(deltaRecord.record);
                        if (writer == null) {
                            writer = new DeltaWriter(sink.config, sink.appId, schema);
                        }
                        writer.writeAvroRecord(r, schema);
                        break;
                    case JSON:
                        r = convertJsontoAvroGenericRecord(deltaRecord.record, schema);
                        if (writer == null) {
                            writer = new DeltaWriter(sink.config, sink.appId, schema);
                        }
                        writer.writeAvroRecord(r, schema);
                        break;
                    default:
                        log.info("not support this kind of schema", deltaRecord.record.getValue().getSchemaType());
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
