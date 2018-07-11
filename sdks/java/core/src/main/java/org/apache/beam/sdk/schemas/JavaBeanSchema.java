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

package org.apache.beam.sdk.schemas;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.utils.JavaBeanGetterFactory;
import org.apache.beam.sdk.schemas.utils.JavaBeanSetterFactory;
import org.apache.beam.sdk.schemas.utils.JavaBeanUtils;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A {@link SchemaProvider} for Java Bean objects.
 *
 * <p>This provider finds (recursively) all public getters and setters in a Java object, and creates
 * schemas and rows that bind to those fields. The field order in the schema is not guaranteed to
 * match the method order in the class.
 */
@Experimental(Kind.SCHEMAS)
public class JavaBeanSchema extends GetterBasedSchemaProvider {
  @Override
  public <T> Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
    return JavaBeanUtils.schemaFromJavaBeanClass(typeDescriptor.getRawType());
  }

  @Override
  public FieldValueGetterFactory fieldValueGetterFactory() {
    return new JavaBeanGetterFactory();
  }

  @Override
  public FieldValueSetterFactory fieldValueSetterFactory() {
    return new JavaBeanSetterFactory();
  }
}
