package org.apache.beam.testinfra.pipelines.schemas;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.google.dataflow.v1beta3.Job;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor.JavaType;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.junit.jupiter.api.Test;

class DescriptorSchemaRegistryTest {
  private static final DescriptorSchemaRegistry.Builder BUILDER
      = new DescriptorSchemaRegistry.Builder();
  @Test
  void buildType_Builder_booleanJavaType() {
    FieldDescriptor booleanField = checkStateNotNull(Job.getDescriptor().findFieldByName("satisfies_pzs"));
    assertEquals(FieldType.BOOLEAN, BUILDER.build(booleanField));
  }

  @Test
  void buildType_Builder_stringJavaType() {

  }
}