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
package org.beam.sdk.java.sql.schema.kafka;

import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.beam.sdk.java.sql.schema.BeamSQLRow;

public class KafkaCSVRecordTransform {
  
  
  public static class ReaderTransform extends PTransform<PCollection<KafkaRecord<byte[], byte[]>>, PCollection<BeamSQLRow>>{
    /**
     * 
     */
    private static final long serialVersionUID = 7613394830984433222L;

    @Override
    public PCollection<BeamSQLRow> expand(PCollection<KafkaRecord<byte[], byte[]>> input) {
      // TODO Auto-generated method stub
      return null;
    }
    
  }
  
  public static class SinkerTransform extends PTransform<PCollection<BeamSQLRow>, PCollection<KafkaRecord<byte[], byte[]>>>{

    /**
     * 
     */
    private static final long serialVersionUID = -722396312765710736L;

    @Override
    public PCollection<KafkaRecord<byte[], byte[]>> expand(PCollection<BeamSQLRow> input) {
      // TODO Auto-generated method stub
      return null;
    }
    
  }

}
