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
package org.apache.beam.it.gcp.artifacts.matchers;

import static com.google.common.truth.Truth.assertAbout;
import static org.apache.beam.it.truthmatchers.PipelineAsserts.assertThatRecords;

import java.util.List;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.it.gcp.artifacts.Artifact;
import org.apache.beam.it.truthmatchers.RecordsSubject;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableList;

public class ArtifactAsserts {

  /**
   * Creates an {@link ArtifactsSubject} to assert information within a list of artifacts obtained
   * from Cloud Storage.
   *
   * @param artifacts Artifacts in list format to use in the comparisons.
   * @return Truth Subject to chain assertions.
   */
  public static ArtifactsSubject assertThatArtifacts(List<Artifact> artifacts) {
    return assertAbout(ArtifactsSubject.records()).that(artifacts);
  }

  /**
   * Creates an {@link ArtifactsSubject} to assert information for an artifact obtained from Cloud
   * Storage.
   *
   * @param artifact Artifact to use in the comparisons.
   * @return Truth Subject to chain assertions.
   */
  public static ArtifactsSubject assertThatArtifact(Artifact artifact) {
    return assertAbout(ArtifactsSubject.records()).that(ImmutableList.of(artifact));
  }

  /**
   * Creates an {@link RecordsSubject} to assert information within a list of records.
   *
   * @param records Records in Avro/Parquet {@link GenericRecord} format to use in the comparison.
   * @return Truth Subject to chain assertions.
   */
  public static RecordsSubject assertThatGenericRecords(List<GenericRecord> records) {
    return assertThatRecords(ArtifactsSubject.genericRecordToRecords(records));
  }
}
