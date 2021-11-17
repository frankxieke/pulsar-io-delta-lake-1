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

import lombok.Data;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The delta checkpoint position.
 */
@Data
public class DeltaCheckpoint implements Comparable<DeltaCheckpoint> {
    private static final Logger log = LoggerFactory.getLogger(DeltaCheckpoint.class);

    public static final String CheckpointStateKeyFormat = "checkpoint%d";
    public static final Long LatestSnapShotVersion = -1L;

    /**
     * StateType is the stage type of CDC copy.
     */
    public enum StateType {
        FULL_COPY("fullcopy"), INCREMENTAL_COPY("incrcopy");
        private String state;
        StateType(String state){
            this.state = state;
        }
    }

    public static String getStatekey(int partition) {
        return String.format(CheckpointStateKeyFormat, partition);
    }

    public DeltaCheckpoint(StateType state) {
        new DeltaCheckpoint(state, LatestSnapShotVersion);
    }

    public DeltaCheckpoint(StateType state, Long snapshotVersion) {
        this.state = state;
        this.snapShotVersion = snapshotVersion;
        this.metadataChangeFileIndex = 0L;
        this.rowNum = 0L;
        this.seqCount = 0L;
    }

    @Override
    public String toString() {
        return "DeltaCheckpoint{"
                + "state=" + state
                + ", snapShotVersion=" + snapShotVersion
                + ", metadataChangeFileIndex=" + metadataChangeFileIndex
                + ", rowNum=" + rowNum
                + ", seqCount=" + seqCount
                + '}';
    }

    private StateType state;
    private Long snapShotVersion;
    private Long metadataChangeFileIndex;
    private Long rowNum;
    private Long seqCount;

    @Override
    public int compareTo(DeltaCheckpoint o) {
        if (!this.state.equals(o.state)) {
            if (this.state.equals(StateType.FULL_COPY)) {
                return -1;
            } else {
                return 1;
            }
        }
        if (!snapShotVersion.equals(o.snapShotVersion)) {
            return snapShotVersion.compareTo(o.snapShotVersion);
        }
        if (!metadataChangeFileIndex.equals(o.metadataChangeFileIndex)) {
            return metadataChangeFileIndex.compareTo(o.metadataChangeFileIndex);
        }
        return rowNum.compareTo(o.rowNum);
    }
    public int compareVersionAndIndex(DeltaCheckpoint o) {
        if (!this.state.equals(o.state)) {
            if (this.state.equals(StateType.FULL_COPY)) {
                return -1;
            } else {
                return 1;
            }
        }
        if (!snapShotVersion.equals(o.snapShotVersion)) {
            return snapShotVersion.compareTo(o.snapShotVersion);
        }

        return metadataChangeFileIndex.compareTo(o.metadataChangeFileIndex);
    }

    public Boolean isFullCopy() {
        if (this.state.equals(StateType.FULL_COPY)) {
            return true;
        }
        return false;
    }
}