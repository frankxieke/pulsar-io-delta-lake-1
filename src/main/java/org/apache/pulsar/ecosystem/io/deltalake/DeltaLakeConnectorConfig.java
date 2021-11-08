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

package org.apache.pulsar.ecosystem.io.deltalake;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The configuration class for {@link DeltaLakeConnectorSource}.
 */

@Data
public class DeltaLakeConnectorConfig implements Serializable {
    private static final Logger log = LoggerFactory.getLogger(DeltaLakeConnectorConfig.class);
    public static final String FromLatest = "latest";
    public static final String FileSystemType = "filesystem";
    public static final String S3Type = "s3";
    String startingVersion;
    String startingTimeStamp;
    Boolean includeHistoryData;
    String tablePath;
    String fileSystemType;
    String s3aAccesskey;
    String s3aSecretKey;

    @JsonIgnore
    Long startingTimeStampSecond;
    @JsonIgnore
    Long startingSnapShotVersionNumber;

    public DeltaLakeConnectorConfig() {
        this.startingVersion = "";
        this.startingTimeStamp = "";
        this.tablePath = "";
        this.fileSystemType = "";
        this.s3aSecretKey = "";
        this.s3aAccesskey = "";
    }

    /**
     * Validate if the configuration is valid.
     */
    public void validate() {
    }

    /**
     * Load the configuration from provided properties.
     *
     * @param config property map
     * @return a loaded {@link DeltaLakeConnectorConfig}.
     * @throws IOException when fail to load the configuration from provided properties
     */
    public static DeltaLakeConnectorConfig load(Map<String, Object> config) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        DeltaLakeConnectorConfig conf = new DeltaLakeConnectorConfig();
        conf.includeHistoryData = false;
        conf = mapper.readValue(new ObjectMapper().writeValueAsString(config), DeltaLakeConnectorConfig.class);
        if (conf.startingTimeStamp.equals("") && conf.startingVersion.equals("")) {
            conf.startingVersion = FromLatest;
        }
        if (!conf.startingVersion.equals("") && !conf.startingTimeStamp.equals("")) {
            throw new IOException("startTimeStamp and startVersion can not be set at the same time.");
        }

        if (!conf.startingVersion.equals("")) {
            if (conf.startingVersion.equals(FromLatest)) {
                conf.startingSnapShotVersionNumber = -1L;
            } else {
                try {
                    conf.startingSnapShotVersionNumber = Long.parseLong(conf.startingVersion);
                } catch (Exception e) {
                    log.info("parse the startingVersion {} failed ", e);
                    throw new IOException("startingVersion should be a number, parse failed");
                }
            }
        }
        if (!conf.startingTimeStamp.equals("")) {
            try {
                Instant instant = Instant.parse(conf.startingTimeStamp);
                conf.startingTimeStampSecond = instant.getEpochSecond();
            } catch (Exception e) {
                log.error("parse the startingTimestamp {} failed, ", e);
                throw new IOException("startingTimestamp format parse failed, "
                        + "it should be like 2021-09-29T20:17:46.384Z");
            }
        }

        if (conf.tablePath.equals("")) {
            throw new IOException("tablePath can not be empty");
        }

        if (conf.fileSystemType.equals("")) {
            throw new IOException("filesystemtype can not be empty");
        }

        if (!conf.fileSystemType.equals(FileSystemType) && !conf.fileSystemType.equals(S3Type)) {
            throw new IOException("fileSystemType not support for " + conf.fileSystemType);
        }
        if (conf.fileSystemType.equals(S3Type)) {
            if (conf.s3aAccesskey.equals("") || conf.s3aSecretKey.equals("")) {
                throw new IOException("s3aAccesskey or s3aSecretkey should be configured for s3");
            }
        }
        return conf;
    }
}
