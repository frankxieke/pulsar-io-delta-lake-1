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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import org.apache.pulsar.ecosystem.io.deltalake.source.DeltaLakeSourceConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The configuration class for {@link DeltaLakeConnectorSink}.
 */
public class DeltaLakeSinkConnectorConfig implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(DeltaLakeSourceConnectorConfig.class);
    public static final Long MAX_WAIT_FLUSH_SECONDS_INTERVAL = 120L;
    public static final Long MAX_PARQUET_FILE_BYTES_SIZE = 10 * 1024L;
    public static final String FileSystemType = "filesystem";
    public static final String S3Type = "s3";
    public String tablePath;
    public List<String> partitionColumns;
    public Long maxWaitFlushSecondsInterval;
    public Long maxParquetFileBytesSize;
    public String fileSystemType;
    public String s3aAccesskey;
    public String s3aSecretKey;


    public DeltaLakeSinkConnectorConfig() {
        this.tablePath = "";
        this.partitionColumns = null;
        this.maxWaitFlushSecondsInterval = MAX_WAIT_FLUSH_SECONDS_INTERVAL;
        this.maxParquetFileBytesSize = MAX_PARQUET_FILE_BYTES_SIZE;
        this.fileSystemType = "";
        this.s3aSecretKey = "";
        this.s3aAccesskey = "";
    }
    /**
     * Validate if the configuration is valid.
     */
    public void validate() throws IOException {
        if (maxWaitFlushSecondsInterval <= 0) {
            maxWaitFlushSecondsInterval = MAX_WAIT_FLUSH_SECONDS_INTERVAL;
        }
        if (maxParquetFileBytesSize <= 0) {
            maxParquetFileBytesSize = MAX_PARQUET_FILE_BYTES_SIZE;
        }

        return;
    }
    /**
     * Load the configuration from provided properties.
     *
     * @param config property map
     * @return a loaded {@link DeltaLakeSinkConnectorConfig}.
     * @throws IOException when fail to load the configuration from provided properties
     */
    public static DeltaLakeSinkConnectorConfig load(Map<String, Object> config) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        DeltaLakeSinkConnectorConfig conf = null;
        conf = mapper.readValue(new ObjectMapper().writeValueAsString(config), DeltaLakeSinkConnectorConfig.class);
        return conf;
    }

}
