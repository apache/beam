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
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.beam.sdk.java.sql.schema.BeamSQLRecordType;
import org.beam.sdk.java.sql.schema.BeamSQLRow;

import io.ebay.rheos.kafka.client.StreamConnectorConfig;
import io.ebay.rheos.schema.avro.RheosEventDeserializer;
import io.ebay.rheos.schema.avro.SchemaRegistryAwareAvroDeserializerHelper;
import io.ebay.rheos.schema.event.RheosEvent;

public class RheosSourceTransform extends PTransform<PCollection<KV<byte[], byte[]>>, PCollection<BeamSQLRow>>{
  /**
   * 
   */
  private static final long serialVersionUID = -7803885128685230359L;
  private BeamSQLRecordType recordType;
  

  public RheosSourceTransform(BeamSQLRecordType recordType) {
    super();
    this.recordType = recordType;
  }


  @Override
  public PCollection<BeamSQLRow> expand(PCollection<KV<byte[], byte[]>> input) {
    return input.apply("soureDecode", ParDo.of(new DoFn<KV<byte[], byte[]>, BeamSQLRow>(){
      transient RheosEventDeserializer rheosDeserializer;
      transient SchemaRegistryAwareAvroDeserializerHelper<GenericRecord> deserializerHelper;
      
      @Setup
      public void setup() {
        this.rheosDeserializer = new RheosEventDeserializer();

        Map<String, Object> config = new HashMap<>();
        config.put(StreamConnectorConfig.RHEOS_SERVICES_URLS, "https://rheos-services.qa.ebay.com");

        deserializerHelper = new SchemaRegistryAwareAvroDeserializerHelper<>(config,
            GenericRecord.class);
      }
      
      @ProcessElement
      public void processElement(ProcessContext c) throws IOException {
        byte[] rawBytes = c.element().getValue();
        RheosEvent event = rheosDeserializer.deserialize("", rawBytes);

        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(
            deserializerHelper.getSchema(event.getSchemaId()));
        Decoder decoder = DecoderFactory.get().binaryDecoder(event.toBytes(), null);

        GenericRecord record = reader.read(null, decoder);
        Map<String, String> k2k = new HashMap<>();
        for (Field f : record.getSchema().getFields()) {
          k2k.put(f.name().toUpperCase(), f.name());
        }

//        BeamSQLRecordType recordType = BeamSQLRecordType.from(protoRowType.apply(new JavaTypeFactoryImpl()));
        BeamSQLRow values = new BeamSQLRow(recordType);
        for (int idx = 0; idx < recordType.getFieldsName().size(); ++idx) {
          values.addField(recordType.getFieldsName().get(idx),
              record.get(k2k.get(recordType.getFieldsName().get(idx))));
        }

        c.output(values);
      }
      
    }));
  }

}
