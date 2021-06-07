package org.apache.beam.sdk.io.gcp.spanner.cdc.model;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.DatabaseClient;
import com.google.cloud.spanner.Mutation;
import com.google.common.collect.ImmutableList;

public class PartitionMetadataDao {

  // Metadata table column names
  public static final String COLUMN_PARTITION_TOKEN = "PartitionToken";
  public static final String COLUMN_PARENT_TOKEN = "ParentToken";
  public static final String COLUMN_START_TIMESTAMP = "StartTimestamp";
  public static final String COLUMN_INCLUSIVE_START = "InclusiveStart";
  public static final String COLUMN_END_TIMESTAMP = "EndTimestamp";
  public static final String COLUMN_INCLUSIVE_END = "InclusiveEnd";
  public static final String COLUMN_HEARTBEAT_SECONDS = "HeartbeatSeconds";
  public static final String COLUMN_STATE = "State";
  public static final String COLUMN_CREATED_AT = "CreatedAt";
  public static final String COLUMN_UPDATED_AT = "UpdatedAt";

  private final DatabaseClient databaseClient;

  public PartitionMetadataDao (DatabaseClient databaseClient) {
    this.databaseClient = databaseClient;
  }

  public Timestamp insert(String table, PartitionMetadata partitionMetadata) {
    Mutation mutation = Mutation.newInsertBuilder(table)
        .set(COLUMN_PARTITION_TOKEN)
        .to(partitionMetadata.getPartitionToken())
        .set(COLUMN_PARENT_TOKEN)
        .toStringArray(partitionMetadata.getParentTokens())
        .set(COLUMN_START_TIMESTAMP)
        .to(partitionMetadata.getStartTimestamp())
        .set(COLUMN_INCLUSIVE_START)
        .to(partitionMetadata.isInclusiveStart())
        .set(COLUMN_END_TIMESTAMP)
        .to(partitionMetadata.getEndTimestamp())
        .set(COLUMN_INCLUSIVE_END)
        .to(partitionMetadata.isInclusiveEnd())
        .set(COLUMN_HEARTBEAT_SECONDS)
        .to(partitionMetadata.getHeartbeatSeconds())
        .set(COLUMN_STATE)
        .to(partitionMetadata.getState().toString())
        .set(COLUMN_CREATED_AT)
        .to(partitionMetadata.getCreatedAt())
        .set(COLUMN_UPDATED_AT)
        .to(partitionMetadata.getUpdatedAt())
        .build();
    return databaseClient.write(ImmutableList.of(mutation));
  }
}
