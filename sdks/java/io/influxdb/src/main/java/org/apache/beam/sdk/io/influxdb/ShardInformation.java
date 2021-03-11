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
import java.util.List;

/** POJO structure for shard information. */
class ShardInformation implements Serializable {

  private double id;
  private String retentionPolicy;
  private double shardGroup;
  private String startTime;
  private String endTime;

  ShardInformation(List<Object> data) {
    id = Double.parseDouble(data.get(0).toString());
    retentionPolicy = data.get(2).toString();
    shardGroup = Double.parseDouble(data.get(3).toString());
    startTime = data.get(4).toString();
    endTime = data.get(5).toString();
  }

  public double getId() {
    return id;
  }

  public String getRetentionPolicy() {
    return retentionPolicy;
  }

  public double getShardGroup() {
    return shardGroup;
  }

  public String getStartTime() {
    return startTime;
  }

  public String getEndTime() {
    return endTime;
  }
}
