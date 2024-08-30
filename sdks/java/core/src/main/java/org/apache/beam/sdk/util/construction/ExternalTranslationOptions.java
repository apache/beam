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
package org.apache.beam.sdk.util.construction;

import java.util.List;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.resourcehints.ResourceHintsOptions.EmptyListDefault;

public interface ExternalTranslationOptions extends PipelineOptions {

  @Description(
      "Set of URNs of transforms to be overriden using the transform service. The provided strings "
          + "can be transform URNs of schema-transform IDs")
  @Default.InstanceFactory(EmptyListDefault.class)
  List<String> getTransformsToOverride();

  void setTransformsToOverride(List<String> transformsToOverride);

  @Description("Address of an already available transform service.")
  String getTransformServiceAddress();

  void setTransformServiceAddress(String transformServiceAddress);

  @Description("An available Beam version which will be used to start a transform service.")
  String getTransformServiceBeamVersion();

  void setTransformServiceBeamVersion(String transformServiceBeamVersion);
}
