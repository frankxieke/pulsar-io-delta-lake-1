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

import io.netty.util.concurrent.DefaultThreadFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.functions.api.Record;
import org.apache.pulsar.io.core.Sink;
import org.apache.pulsar.io.core.SinkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A sink connector that read from pulsar and write to delta lake using delta standalone writer.
 */
public class DeltaLakeConnectorSink  implements Sink<GenericRecord> {
    private static final Logger log = LoggerFactory.getLogger(DeltaLakeConnectorSink.class);
    public DeltaLakeSinkConnectorConfig config;
    public String appId;
    SinkContext sinkContext;
    private ExecutorService executor;

    public LinkedBlockingQueue<DeltaSinkRecord> queue = new LinkedBlockingQueue<DeltaSinkRecord>(10000);

    @Override
    public void close() throws Exception {
    }

    @Override
    public void open(Map<String, Object> config, SinkContext sinkContext) throws Exception {
        log.info("begin to open");
        CompletableFuture<List<String>> listPartitions =
                sinkContext.getPulsarClient().getPartitionsForTopic((String) sinkContext.getInputTopics().toArray()[0]);
        log.info("topicName {} souceName {} partitionSize: {}", sinkContext.getInputTopics(), sinkContext.getSinkName(),
                listPartitions.get().size());
        this.sinkContext = sinkContext;
        // load the configuration and validate it
        this.config = DeltaLakeSinkConnectorConfig.load(config);
        this.config.validate();
        executor = Executors.newSingleThreadExecutor(new DefaultThreadFactory("deltaWriteThreadPool"));
        executor.execute(new DeltaWriterThread(this));

    }

    @Override
    public void write(Record<GenericRecord> record) throws Exception {
        this.queue.put(new DeltaSinkRecord(record));
    }
}
