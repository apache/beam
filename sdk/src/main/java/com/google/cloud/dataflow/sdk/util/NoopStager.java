/*
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.cloud.dataflow.sdk.util;

import com.google.api.services.dataflow.model.DataflowPackage;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

import java.util.ArrayList;
import java.util.List;

/**
 * Do-nothing stager class. stageFiles() does nothing and returns an empty list of packages.
 */
class NoopStager implements Stager {
  public static NoopStager fromOptions(PipelineOptions options) {
    return new NoopStager();
  }

  @Override
  public List<DataflowPackage> stageFiles() {
    return new ArrayList<DataflowPackage>();
  }
}
