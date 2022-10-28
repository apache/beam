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
package org.apache.beam.sdk.io.cdap;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.common.ReferencePluginConfig;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.io.cdap.batch.EmployeeBatchSink;
import org.apache.beam.sdk.io.cdap.batch.EmployeeBatchSource;

/**
 * {@link io.cdap.cdap.api.plugin.PluginConfig} for {@link EmployeeBatchSource} and {@link
 * EmployeeBatchSink} CDAP plugins. Used to test {@link org.apache.beam.sdk.io.cdap.CdapIO#read()}
 * and {@link org.apache.beam.sdk.io.cdap.CdapIO#write()}.
 */
public class EmployeeConfig extends ReferencePluginConfig {

  public static final String OBJECT_TYPE = "objectType";

  @Name(OBJECT_TYPE)
  @Description("Name of Object(s) to pull.")
  @Macro
  public String objectType;

  public EmployeeConfig(String objectType, String referenceName) {
    super(referenceName);
    this.objectType = objectType;
  }

  public Schema getSchema() {
    Set<Schema.Field> schemaFields = new HashSet<>();
    schemaFields.add(Schema.Field.of("id", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    schemaFields.add(Schema.Field.of("name", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    return Schema.recordOf("etlSchemaBody", schemaFields);
  }

  public void validate(FailureCollector failureCollector) {
    if (containsMacro(OBJECT_TYPE)) {
      return;
    }
    if (objectType == null) {
      failureCollector
          .addFailure("Object Type must be not null.", null)
          .withConfigProperty(OBJECT_TYPE);
    }
  }
}
