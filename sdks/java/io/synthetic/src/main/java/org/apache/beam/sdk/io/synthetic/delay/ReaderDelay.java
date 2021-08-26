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
package org.apache.beam.sdk.io.synthetic.delay;

import java.util.Random;
import org.apache.beam.sdk.io.Source;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions;
import org.apache.beam.sdk.io.synthetic.SyntheticSourceOptions.Record;

/** Imposes delays in synthetic {@link Source.Reader} classes. */
public class ReaderDelay {

  private SyntheticSourceOptions options;

  public ReaderDelay(SyntheticSourceOptions options) {
    this.options = options;
  }

  public void delayRecord(Record record) {
    // TODO: add a separate distribution for the sleep time of reading the first record
    // (e.g.,"open" the files).
    long hashCodeOfVal = options.hashFunction().hashBytes(record.kv.getValue()).asLong();
    Random random = new Random(hashCodeOfVal);
    SyntheticDelay.delay(
        record.sleepMsec, options.cpuUtilizationInMixedDelay, options.delayType, random);
  }

  public void delayStart(long currentElement) {
    SyntheticDelay.delay(
        options.nextInitializeDelay(currentElement),
        options.cpuUtilizationInMixedDelay,
        options.delayType,
        new Random(currentElement));
  }
}
