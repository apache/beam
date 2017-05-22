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
package org.apache.beam.sdk.io.hive;

//import java.io.DataOutput;
//import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
//import java.util.Properties;

import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.Coder;
//import org.apache.beam.sdk.util.StreamUtils;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hive.serde.serdeConstants;
//import org.apache.hadoop.hive.serde2.SerDeException;
//import org.apache.hadoop.io.BytesWritable;
//import org.apache.hadoop.io.Writable;
//import org.apache.hive.hcatalog.common.HCatConstants;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.HCatRecord;
//import org.apache.hive.hcatalog.data.HCatRecordSerDe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A really simple {@link Coder} that serializes and deserializes the {@link HCatRecord}
 * treating them as space delimited strings.
 */
class HCatRecordSimpleCoder extends AtomicCoder<HCatRecord> implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(HCatRecordSimpleCoder.class);
  private static final HCatRecordSimpleCoder INSTANCE = new HCatRecordSimpleCoder();
  private static String metastoreUri;

  private HCatRecordSimpleCoder() {}

  public static HCatRecordSimpleCoder of(String metastoreUri) {
    HCatRecordSimpleCoder.metastoreUri = metastoreUri;
    return INSTANCE;
  }

  @Override
  public void encode(HCatRecord value, OutputStream outputStream)
          throws IOException {
    //TODO: Use HCatRecordSerDe to do a generic encode
    outputStream.write(value.toString().getBytes());
  }

  @Override
  public HCatRecord decode(InputStream inputStream)
      throws IOException {
    StringBuilder content = new StringBuilder();
    byte[] buffer = new byte[1024];
    int cnt = 0;
    //TODO: Use HCatRecordSerDe to do a generic decode
    while ((cnt = inputStream.read(buffer)) > 0) {
      content.append(new String(buffer));
    }
    List<Object> objList = new ArrayList<Object>(Arrays.asList(content.toString().split(" ")));
    return new DefaultHCatRecord(objList);
  }
}
