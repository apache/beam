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
package org.apache.beam.sdk.io.gcp.healthcare;

import com.google.api.services.healthcare.v1alpha2.model.HttpBody;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;

/**
 * {@link FhirIO} provides an API for writing resources to <a
 * href="https://cloud.google.com/healthcare/docs/concepts/fhir">Google Cloud Healthcare Fhir API.
 * </a>
 */
public class FhirIO {

  /** The type Write. */
  @AutoValue
  public abstract static class Write extends PTransform<PCollection<HttpBody>, PDone> {

    /** The enum Write method. */
    public enum WriteMethod {
      /**
       * Execute Bundle Method executes a batch of requests as a single transaction @see <a
       * href=https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.fhirStores.fhir/executeBundle></a>.
       */
      EXECUTE_BUNDLE,
      /**
       * Create Method creates a single FHIR resource @see <a
       * href=https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.fhirStores.fhir/create></a>.
       */
      CREATE
    }

    /**
     * Gets Fhir store.
     *
     * @return the Fhir store
     */
    abstract String getFhirStore();

    /**
     * Gets write method.
     *
     * @return the write method
     */
    abstract WriteMethod getWriteMethod();

    /** The type Builder. */
    @AutoValue.Builder
    abstract static class Builder {

      /**
       * Sets Fhir store.
       *
       * @param fhirStore the Fhir store
       * @return the Fhir store
       */
      abstract Builder setFhirStore(String fhirStore);

      /**
       * Sets write method.
       *
       * @param writeMethod the write method
       * @return the write method
       */
      abstract Builder setWriteMethod(WriteMethod writeMethod);

      /**
       * Build write.
       *
       * @return the write
       */
      abstract Write build();
    }

    private static Write.Builder write(String fhirStore) {
      return new AutoValue_FhirIO_Write.Builder().setFhirStore(fhirStore);
    }

    /**
     * Create Method creates a single FHIR resource. @see <a
     * href=https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.fhirStores.fhir/create></a>
     *
     * @param fhirStore the hl 7 v 2 store
     * @return the write
     */
    public static Write create(String fhirStore) {
      return new AutoValue_FhirIO_Write.Builder()
          .setFhirStore(fhirStore)
          .setWriteMethod(Write.WriteMethod.CREATE)
          .build();
    }

    /**
     * Execute Bundle Method executes a batch of requests as a single transaction @see <a
     * href=https://cloud.google.com/healthcare/docs/reference/rest/v1beta1/projects.locations.datasets.fhirStores.fhir/executeBundle></a>.
     *
     * @param fhirStore the hl 7 v 2 store
     * @return the write
     */
    public static Write executeBundles(String fhirStore) {
      return new AutoValue_FhirIO_Write.Builder()
          .setFhirStore(fhirStore)
          .setWriteMethod(WriteMethod.EXECUTE_BUNDLE)
          .build();
    }

    @Override
    public PDone expand(PCollection<HttpBody> messages) {
      messages.apply(ParDo.of(new WriteFhirFn(this.getFhirStore(), this.getWriteMethod())));
      return PDone.in(messages.getPipeline());
    }
  }

  /** The type Write Fhir fn. */
  static class WriteFhirFn extends DoFn<HttpBody, PDone> {
    // TODO when the healthcare API releases a bulk import method this should use that to improve
    // throughput.

    private transient HealthcareApiClient client;
    /** The Fhir store. */
    private final String fhirStore;
    /** The Write method. */
    private final Write.WriteMethod writeMethod;

    /**
     * Instantiates a new Write Fhir fn.
     *
     * @param fhirStore the Fhir store
     * @param writeMethod the write method
     */
    WriteFhirFn(String fhirStore, Write.WriteMethod writeMethod) {
      this.fhirStore = fhirStore;
      this.writeMethod = writeMethod;
    }

    /**
     * Init client.
     *
     * @throws IOException the io exception
     */
    @Setup
    void initClient() throws IOException {
      this.client = new HttpHealthcareApiClient();
    }

    /**
     * Write messages.
     *
     * @param context the context
     * @throws IOException the io exception
     */
    @ProcessElement
    void writeBundles(ProcessContext context) throws IOException {
      HttpBody body = context.element();
      //  TODO could insert some lineage hook here?
      switch (this.writeMethod) {
        case CREATE:
          client.createFhirResource(fhirStore, (String) body.get("type"), body);
          break;
        case EXECUTE_BUNDLE:
        default:
          client.executeFhirBundle(fhirStore, body);
      }
    }
  }
}
