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

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubMessage;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

public class DicomIO {

  /**
   * Read dicom study metadata from a PCollection of resource webpath.
   *
   * @return the read
   * @see ReadDicomStudyMetadata
   */
  public static ReadDicomStudyMetadata retrieveStudyMetadata() {
    return new ReadDicomStudyMetadata();
  }

  /** The type ReadDicomStudyMetadata. */
  public static class ReadDicomStudyMetadata
      extends PTransform<PCollection<PubsubMessage>, PCollection<String>> {

    public ReadDicomStudyMetadata() {}

    public static final TupleTag<String> OUT = new TupleTag<String>() {};

    /**
     * DoFn to fetch the metadata of a study from a Dicom store based on it's location and study id.
     */
    static class FetchStudyMetadataFn extends DoFn<PubsubMessage, String> {

      private HealthcareApiClient dicomStore;

      /**
       * Instantiate the healthcare client.
       *
       * @throws IOException
       */
      @Setup
      public void instantiateHealthcareClient() throws IOException {
        this.dicomStore = new HttpHealthcareApiClient();
      }

      /**
       * Process The Pub/Sub message.
       *
       * @param context The input containing the pub/sub message
       */
      @ProcessElement
      public void processElement(ProcessContext context) {
        PubsubMessage msg = context.element();
        byte[] msgPayload = msg.getPayload();
        try {
          String dicomWebPath = new String(msgPayload, "UTF-8");
          String responseData = dicomStore.retrieveDicomStudyMetadata(dicomWebPath);
          context.output(responseData);
        } catch (Exception e) {
          // IO exception, unsupported encoding exception
          System.out.println(e);
          if (e.getClass() == IOException.class) {
          } else if (e.getClass() == UnsupportedEncodingException.class) {
          } else {
          }
        }
      }
    }

    @Override
    public PCollection<String> expand(PCollection<PubsubMessage> input) {
      return input.apply(ParDo.of(new FetchStudyMetadataFn()));
    }
  }
}
