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

import java.util.HashMap;
import java.util.Map;
import junit.framework.TestCase;


/**
 * Test config.
 */
public class DeltaLakeConnectorConfigTest extends TestCase {

    public void testLoad() {
        Map<String, Object> map = new HashMap<>();
        map.put("startingVersion", "1");
        map.put("includeHistoryData", true);
        map.put("tablePath", "/tmp/delta_stand_alone_test");
        try {
            DeltaLakeConnectorConfig config = DeltaLakeConnectorConfig.load(map);
            System.out.println(config.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}