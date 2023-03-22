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
package org.apache.beam.sdk.extensions.avro.schemas;
@org.apache.avro.specific.FixedSize(4)
@org.apache.avro.specific.AvroGenerated
public class fixed4 extends org.apache.avro.specific.SpecificFixed {
  private static final long serialVersionUID = -5646354132642432749L;
  public static final org.apache.avro.Schema SCHEMA$ = new org.apache.avro.Schema.Parser().parse("{\"type\":\"fixed\",\"name\":\"fixed4\",\"namespace\":\"org.apache.beam.sdk.extensions.avro.schemas\",\"size\":4}");
  public static org.apache.avro.Schema getClassSchema() { return SCHEMA$; }
  public org.apache.avro.Schema getSchema() { return SCHEMA$; }

  /** Creates a new fixed4 */
  public fixed4() {
    super();
  }

  /**
   * Creates a new fixed4 with the given bytes.
   * @param bytes The bytes to create the new fixed4.
   */
  public fixed4(byte[] bytes) {
    super(bytes);
  }

  private static final org.apache.avro.io.DatumWriter<fixed4>
    WRITER$ = new org.apache.avro.specific.SpecificDatumWriter<fixed4>(SCHEMA$);

  @Override public void writeExternal(java.io.ObjectOutput out)
    throws java.io.IOException {
    WRITER$.write(this, org.apache.avro.specific.SpecificData.getEncoder(out));
  }

  private static final org.apache.avro.io.DatumReader<fixed4>
    READER$ = new org.apache.avro.specific.SpecificDatumReader<fixed4>(SCHEMA$);

  @Override public void readExternal(java.io.ObjectInput in)
    throws java.io.IOException {
    READER$.read(this, org.apache.avro.specific.SpecificData.getDecoder(in));
  }

}
