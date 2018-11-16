/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.extensions.timeseries;

import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/** Pipeline options to configure timeseries-related functionality. */
@Experimental
public interface TimeSeriesOptions extends PipelineOptions {

  @Description("Back fill option that should be used in pipeline")
  String getFillOption();

  void setFillOption(String option);

  @Description(
      "The down sample period in milli seconds, which must be set. Default is 60 Sec (Minimum Resolution 100ms) ")
  @Default.Long(60000)
  Long getDownSampleDurationMillis();

  void setDownSampleDurationMillis(Long mills);

  @Description(
      "Once a key is observed we will generate a value during periods when the key has not been observed if the fillOption is set to anything other than NONE.")
  @Default.Long(0)
  Long getTimeToLiveMillis();

  void setTimeToLiveMillis(Long mills);

  @Description("Discard incomplete sequence from sequenced output.")
  @Default.Boolean(false)
  Boolean getDiscardIncompleteSequences();

  void setDiscardIncompleteSequences(Boolean discardIncompleteSequences);

  @Description("Allow forward pad of sequenced output. ")
  @Default.Boolean(false)
  Boolean getForwardPadSequencedOutput();

  void setForwardPadSequencedOutput(Boolean forwardPadSequencedOutput);

  /**
   * This value should not normally be used as it can lead to strange output data sets. If the
   * sequence output has not seen a key at the start of the lower window boundary, this will
   * generate 'back' data from the first values seen. This can lead to keys coming into existence
   * when they did not exist in the original data. It is here as a convenience method when playing
   * with data at the sink which needs to avoid having to pad.
   *
   * @return
   */
  @Description("Allow back pad of sequenced output. This should not normally be set to true.")
  @Default.Boolean(false)
  Boolean getBackPadSequencedOutput();

  void setBackPadSequencedOutput(Boolean backPadSequencedOutput);
}
