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
package org.beam.sdk.java.sql.examples;

import java.io.IOException;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.transforms.DoFn.ProcessElement;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.beam.sdk.java.sql.schema.BeamSQLRecordType;
import org.beam.sdk.java.sql.schema.BeamSQLRow;

public class RheosSinkTransform extends PTransform<PCollection<BeamSQLRow>, PCollection<KV<byte[], byte[]>>>{
  /**
   * 
   */
  private static final long serialVersionUID = 6589679229923907701L;
  private BeamSQLRecordType recordType;
  

  public RheosSinkTransform(BeamSQLRecordType recordType) {
    super();
    this.recordType = recordType;
  }


  @Override
  public PCollection<KV<byte[], byte[]>> expand(PCollection<BeamSQLRow> input) {
    return input.apply("toKafkaRecord", ParDo.of(new DoFn<BeamSQLRow, KV<byte[], byte[]>>(){
      @ProcessElement
      public void processElement(ProcessContext c) throws IOException {
        c.output(KV.of(new byte[]{}, c.element().getDataMap().toString().getBytes()));
      }
    }));
  }

}
