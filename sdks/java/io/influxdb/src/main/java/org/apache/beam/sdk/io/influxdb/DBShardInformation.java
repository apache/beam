/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.influxdb;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.influxdb.dto.QueryResult.Series;

/**
 * Utility class to load a ShardInformation from the Series Data. Shard POJO class is defined in
 * {@code ShardInformation}.
 */
class DBShardInformation implements Serializable {
  /** Map Structure with database as key and shardInformation as Value. */
  private Map<String, List<ShardInformation>> shardInformation = new HashMap<>();

  DBShardInformation() {}

  public void loadShardInformation(String dbName, Series sData) {
    List<ShardInformation> shardInfo;
    if (!shardInformation.containsKey(dbName)) {
      shardInfo = new ArrayList<>();
    } else {
      shardInfo = shardInformation.get(dbName);
    }
    if (sData.getValues() != null && sData.getName().equals(dbName)) {
      for (List<Object> data : sData.getValues()) {
        if (data != null) {
          shardInfo.add(new ShardInformation(data));
        }
      }
    }
    shardInformation.put(dbName, shardInfo);
  }

  public List<ShardInformation> getShardInformation(String dbName) {
    if (shardInformation.containsKey(dbName)) {
      return shardInformation.get(dbName);
    } else {
      return Collections.emptyList();
    }
  }
}
