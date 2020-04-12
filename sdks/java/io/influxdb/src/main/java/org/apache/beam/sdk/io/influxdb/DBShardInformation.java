package org.apache.beam.sdk.io.influxdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.influxdb.dto.QueryResult.Series;

public class DBShardInformation {

  private Map<String, List<ShardInformation>> shardInformation = new HashMap<>();

  public DBShardInformation() {}

  public void loadShardInformation(String dbName, Series sData) {
    if (!shardInformation.containsKey(dbName)) {
      shardInformation.put(dbName, new ArrayList<>());
    }
    if (sData.getName().equals(dbName)) {
      List<ShardInformation> shardInfo = shardInformation.get(dbName);
      for (List<Object> data : sData.getValues()) {
        shardInfo.add(new ShardInformation(data));
      }
      shardInformation.put(dbName, shardInfo);
    }
  }

  public List<ShardInformation> getShardInformation(String dbName) {
    return shardInformation.get(dbName);
  }
}
