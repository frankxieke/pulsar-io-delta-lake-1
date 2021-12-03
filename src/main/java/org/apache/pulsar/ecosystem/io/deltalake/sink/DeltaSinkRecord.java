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

import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The delta sink Record.
 */
public class DeltaSinkRecord {
    private static final Logger log = LoggerFactory.getLogger(DeltaSinkRecord.class);
    public Record<GenericRecord> record;
    public int partition;
    public MessageId messageId;
    public long sequenceId;
    public long timestamp;

    public DeltaSinkRecord(Record<GenericRecord> record) {
        if (record != null) {
            this.record = record;
            this.partition = record.getPartitionIndex().get();
            this.messageId = record.getMessage().get().getMessageId();
            this.sequenceId = record.getMessage().get().getSequenceId();
        }
        this.timestamp = System.currentTimeMillis();
    }
}
