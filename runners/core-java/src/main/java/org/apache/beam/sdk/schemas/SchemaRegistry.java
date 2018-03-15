package org.apache.beam.sdk.schemas;

import org.apache.beam.sdk.values.TypeDescriptor;

public class SchemaRegistry {
  public void registerScheamForClass(Class<?> clazz) {

  }

  public void registerSchemaForType(TypeDescriptor<?> type) {

  }

  public void registerSchemaProvider(SchemaProvider schemaProvider) {

  }

  public <T> Schema getSchema(Class<T> clazz) throws NoSuchSchemaException {

  }

  public <T> Schema getSchema(TypeDescriptor<T> typeDescriptor) throws NoSuchSchemaException {

  }
}
