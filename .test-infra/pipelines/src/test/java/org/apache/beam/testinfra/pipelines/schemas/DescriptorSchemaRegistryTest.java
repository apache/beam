package org.apache.beam.testinfra.pipelines.schemas;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.dataflow.v1beta3.Job;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

class DescriptorSchemaRegistryTest {

  private static final DescriptorSchemaRegistry REGISTRY = new DescriptorSchemaRegistry();

  @BeforeAll
  public static void setup() {
    REGISTRY.build(Job.getDescriptor());
  }

  @Test
  void build_Job() {
    Schema schema = REGISTRY.getOrBuild(Job.getDescriptor());
    assertEquals(Schema.Field.of("id", FieldType.STRING), schema.getField("id"));
    assertEquals(Schema.Field.of("type", FieldType.STRING), schema.getField("type"));
    assertEquals(Schema.Field.of("create_time", FieldType.DATETIME), schema.getField("create_time"));
    assertEquals(
            Schema.Field.of("steps", FieldType.array(
                    FieldType.row(Schema.of(
                            Schema.Field.of("kind", FieldType.STRING),
                            Schema.Field.of("name", FieldType.STRING),
                            Schema.Field.of("properties", FieldType.row(Schema.of(
                                    Schema.Field.of("fields", FieldType.map(FieldType.STRING, FieldType.STRING))
                            )))
                    ))
            )),
            schema.getField("steps")
    );
    assertEquals(Schema.Field.of("transform_name_mapping", FieldType.map(FieldType.STRING, FieldType.STRING)), schema.getField("transform_name_mapping"));
    assertEquals(Schema.Field.of("stage_states", FieldType.array(FieldType.row(Schema.of(
                    Schema.Field.of("execution_stage_name", FieldType.STRING),
            Schema.Field.of("execution_stage_state", FieldType.STRING),
            Schema.Field.of("current_state_time", FieldType.DATETIME)
            )))),
            schema.getField("stage_states"));
    assertEquals(Schema.Field.of("satisfies_pzs", FieldType.BOOLEAN), schema.getField("satisfies_pzs"));
  }
}