package org.apache.beam.sdk.io.influxdb;

import java.util.List;
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
}
