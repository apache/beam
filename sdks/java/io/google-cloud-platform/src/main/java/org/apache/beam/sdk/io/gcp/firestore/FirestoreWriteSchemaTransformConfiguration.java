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
package org.apache.beam.sdk.io.gcp.firestore;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.sdk.schemas.transforms.providers.ErrorHandling;
import org.checkerframework.checker.nullness.qual.Nullable;

/** Configuration class for the Firestore Write transform. */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class FirestoreWriteSchemaTransformConfiguration implements Serializable {

  @SchemaFieldDescription("GCP project id. Defaults to GcpOptions project when unset.")
  @Nullable
  public abstract String getProjectId();

  @SchemaFieldDescription(
      "Firestore database id. Defaults to FirestoreOptions firestoreDb when unset.")
  @Nullable
  public abstract String getDatabaseId();

  @SchemaFieldDescription("Firestore collection id to write to.")
  public abstract String getCollectionId();

  @SchemaFieldDescription(
      "Row field containing the document id. Defaults to document_id when unset.")
  @Nullable
  public abstract String getDocumentIdField();

  @SchemaFieldDescription(
      "This option specifies whether and where to output unwritable rows. Error handling is "
          + "limited to data conversion failures before sending writes to Firestore.")
  @Nullable
  public abstract ErrorHandling getErrorHandling();

  public void validate() {
    checkArgument(
        getCollectionId() != null && !getCollectionId().isEmpty(),
        "Firestore collection id must be specified.");
  }

  public static Builder builder() {
    return new AutoValue_FirestoreWriteSchemaTransformConfiguration.Builder();
  }

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setProjectId(String projectId);

    public abstract Builder setDatabaseId(String databaseId);

    public abstract Builder setCollectionId(String collectionId);

    public abstract Builder setDocumentIdField(String documentIdField);

    public abstract Builder setErrorHandling(ErrorHandling errorHandling);

    public abstract FirestoreWriteSchemaTransformConfiguration build();
  }
}
