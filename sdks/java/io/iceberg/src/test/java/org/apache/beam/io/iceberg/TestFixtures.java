package org.apache.beam.io.iceberg;

import static org.apache.iceberg.types.Types.NestedField.optional;
import static org.apache.iceberg.types.Types.NestedField.required;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.iceberg.Schema;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

public class TestFixtures {
  public static final Schema SCHEMA = new Schema(
      required(1,"id", Types.LongType.get()),
      optional(2,"data",Types.StringType.get())
  );

  private static final Record genericRecord = GenericRecord.create(SCHEMA);


  /* First file in test table */
  public static final ImmutableList<Record> FILE1SNAPSHOT1 = ImmutableList.of(
    genericRecord.copy(ImmutableMap.of("id",0L,"data","clarification")),
    genericRecord.copy(ImmutableMap.of("id",1L,"data","risky")),
    genericRecord.copy(ImmutableMap.of("id",2L,"data","falafel"))
  );
  public static final ImmutableList<Record> FILE1SNAPSHOT2 = ImmutableList.of(
      genericRecord.copy(ImmutableMap.of("id",4L,"data","obscure")),
      genericRecord.copy(ImmutableMap.of("id",5L,"data","secure")),
      genericRecord.copy(ImmutableMap.of("id",6L,"data","feta"))
  );
  public static final ImmutableList<Record> FILE1SNAPSHOT3 = ImmutableList.of(
      genericRecord.copy(ImmutableMap.of("id",6L,"data","brainy")),
      genericRecord.copy(ImmutableMap.of("id",7L,"data","film")),
      genericRecord.copy(ImmutableMap.of("id",8L,"data","feta"))
  );

  /* Second file in test table */
  public static final ImmutableList<Record> FILE2SNAPSHOT1 = ImmutableList.of(
      genericRecord.copy(ImmutableMap.of("id",10L,"data","clammy")),
      genericRecord.copy(ImmutableMap.of("id",11L,"data","evacuate")),
      genericRecord.copy(ImmutableMap.of("id",12L,"data","tissue"))
  );
  public static final ImmutableList<Record> FILE2SNAPSHOT2 = ImmutableList.of(
      genericRecord.copy(ImmutableMap.of("id",14L,"data","radical")),
      genericRecord.copy(ImmutableMap.of("id",15L,"data","collocation")),
      genericRecord.copy(ImmutableMap.of("id",16L,"data","book"))
  );
  public static final ImmutableList<Record> FILE2SNAPSHOT3 = ImmutableList.of(
      genericRecord.copy(ImmutableMap.of("id",16L,"data","cake")),
      genericRecord.copy(ImmutableMap.of("id",17L,"data","intrinsic")),
      genericRecord.copy(ImmutableMap.of("id",18L,"data","paper"))
  );

  /* Third file in test table */
  public static final ImmutableList<Record> FILE3SNAPSHOT1 = ImmutableList.of(
      genericRecord.copy(ImmutableMap.of("id",20L,"data","ocean")),
      genericRecord.copy(ImmutableMap.of("id",21L,"data","holistic")),
      genericRecord.copy(ImmutableMap.of("id",22L,"data","preventative"))
  );
  public static final ImmutableList<Record> FILE3SNAPSHOT2 = ImmutableList.of(
      genericRecord.copy(ImmutableMap.of("id",24L,"data","cloud")),
      genericRecord.copy(ImmutableMap.of("id",25L,"data","zen")),
      genericRecord.copy(ImmutableMap.of("id",26L,"data","sky"))
  );
  public static final ImmutableList<Record> FILE23NAPSHOT3 = ImmutableList.of(
      genericRecord.copy(ImmutableMap.of("id",26L,"data","belleview")),
      genericRecord.copy(ImmutableMap.of("id",27L,"data","overview")),
      genericRecord.copy(ImmutableMap.of("id",28L,"data","tender"))
  );

}
