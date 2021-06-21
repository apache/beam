package org.apache.beam.sdk.io.gcp.spanner.cdc.mapper;

import static org.apache.beam.sdk.io.gcp.spanner.cdc.util.TestStructMapper.recordsToStruct;
import static org.junit.Assert.assertEquals;

import com.google.cloud.Timestamp;
import com.google.cloud.spanner.Struct;
import com.google.common.collect.ImmutableMap;
import com.google.gson.Gson;
import java.util.Arrays;
import java.util.Collections;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ChildPartitionsRecord.ChildPartition;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ColumnType;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.DataChangesRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.HeartbeatRecord;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.Mod;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ModType;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.TypeCode;
import org.apache.beam.sdk.io.gcp.spanner.cdc.model.ValueCaptureType;
import org.junit.Before;
import org.junit.Test;

public class ChangeStreamRecordMapperTest {

  private ChangeStreamRecordMapper mapper;

  @Before
  public void setUp() {
    this.mapper = new ChangeStreamRecordMapper(new Gson());
  }

  @Test
  public void testMappingStructRowToDataChangesRecord() {
    final DataChangesRecord dataChangesRecord = new DataChangesRecord(
        "partitionToken",
        Timestamp.ofTimeSecondsAndNanos(10L, 20),
        "transactionId",
        true,
        "1",
        "tableName",
        Arrays.asList(
            new ColumnType("column1", new TypeCode("type1"), true, 1L),
            new ColumnType("column2", new TypeCode("type2"), false, 2L)
        ),
        Collections.singletonList(
            new Mod(
                ImmutableMap.of("column1", "value1"),
                ImmutableMap.of("column2", "oldValue2"),
                ImmutableMap.of("column2", "newValue2")
            )
        ),
        ModType.UPDATE,
        ValueCaptureType.OLD_AND_NEW_VALUES,
        10L,
        2L
    );
    final Struct struct = recordsToStruct(dataChangesRecord);

    assertEquals(
        Collections.singletonList(dataChangesRecord),
        mapper.toChangeStreamRecords("partitionToken", struct)
    );
  }

  @Test
  public void testMappingStructRowToHeartbeatRecord() {
    final HeartbeatRecord heartbeatRecord = new HeartbeatRecord(
        Timestamp.ofTimeSecondsAndNanos(10L, 20)
    );
    final Struct struct = recordsToStruct(heartbeatRecord);

    assertEquals(
        Collections.singletonList(heartbeatRecord),
        mapper.toChangeStreamRecords("partitionToken", struct)
    );
  }

  @Test
  public void testMappingStructRowToChildPartitionRecord() {
    final ChildPartitionsRecord childPartitionsRecord = new ChildPartitionsRecord(
        Timestamp.ofTimeSecondsAndNanos(10L, 20),
        "1",
        Arrays.asList(
            new ChildPartition("childToken1", Arrays.asList("parentToken1", "parentToken2")),
            new ChildPartition("childToken2", Arrays.asList("parentToken1", "parentToken2"))
        )
    );
    final Struct struct = recordsToStruct(childPartitionsRecord);

    assertEquals(
        Collections.singletonList(childPartitionsRecord),
        mapper.toChangeStreamRecords("partitionToken", struct)
    );
  }

  // TODO: Add test case for unknown record type
  // TODO: Add test case for malformed record
}
