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
package org.apache.beam.runners.twister2;

import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.BatchTSetImpl;
import edu.iu.dsc.tws.tset.sets.batch.SinkTSet;
import org.apache.beam.sdk.util.WindowedValue;
import org.apache.beam.sdk.values.PValue;

/** Twister2BatchTranslationContext. */
public class Twister2BatchTranslationContext extends Twister2TranslationContext {

  public Twister2BatchTranslationContext(Twister2PipelineOptions options) {
    super(options);
  }

  @Override
  public <T> BatchTSetImpl<WindowedValue<T>> getInputDataSet(PValue input) {
    BatchTSetImpl<WindowedValue<T>> baseTSet =
        (BatchTSetImpl<WindowedValue<T>>) super.<T>getInputDataSet(input);
    return baseTSet;
  }

  @Override
  public void eval(SinkTSet<?> tSet) {
    ((BatchTSetEnvironment) getEnvironment()).run(tSet);
  }
}
