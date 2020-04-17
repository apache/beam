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

import java.util.List;
import java.util.Objects;
import org.joda.time.DateTime;

public class ShardInformation implements Comparable {

  private double id;
  private String retentionPolicy;
  private double shardGroup;
  private DateTime startTime;
  private DateTime endTime;

  public ShardInformation(List<Object> data) {
    id = Double.parseDouble(data.get(0).toString());
    retentionPolicy = data.get(2).toString();
    shardGroup = Double.parseDouble(data.get(3).toString());
    startTime = DateTime.parse(data.get(4).toString());
    endTime = DateTime.parse(data.get(5).toString());
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

  public DateTime getStartTime() {
    return startTime;
  }

  public DateTime getEndTime() {
    return endTime;
  }

  @Override
  public int compareTo(Object o) {
    return getStartTime().compareTo(((ShardInformation) o).getStartTime());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null) {
      return false;
    }
    if (getClass() != o.getClass()) {
      return false;
    }
    ShardInformation shardInformation = (ShardInformation) o;
    return Objects.equals(id, shardInformation.id)
        && Objects.equals(retentionPolicy, shardInformation.retentionPolicy)
        && Objects.equals(shardGroup, shardInformation.shardGroup)
        && Objects.equals(startTime, shardInformation.startTime)
        && Objects.equals(endTime, shardInformation.endTime);
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, retentionPolicy, shardGroup, startTime, endTime);
  }
}
