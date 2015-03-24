/*
 * Copyright (C) 2015 Google Inc.
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
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Do-nothing stager class. stageFiles() returns a package list for all files in
 * options.getFilesToStage in their original locations.
 */
class NoopStager implements Stager {
  private DataflowPipelineOptions options;

  private NoopStager(DataflowPipelineOptions options) {
      this.options = options;
  }

  public static NoopStager fromOptions(PipelineOptions options) {
    return new NoopStager(options.as(DataflowPipelineOptions.class));
  }

  @Override
  public List<DataflowPackage> stageFiles() {
    ArrayList<DataflowPackage> packages = new ArrayList<>();
    for (String fileName : options.getFilesToStage()) {
      String packageName = null;
      if (fileName.contains("=")) {
        String[] components = fileName.split("=", 2);
        packageName = components[0];
        fileName = components[1];
      }
      if (packageName == null) {
        packageName = PackageUtil.getUniqueContentName(new File(fileName), "");
      }

      DataflowPackage workflowPackage = new DataflowPackage();
      workflowPackage.setName(packageName);
      workflowPackage.setLocation(fileName);
      packages.add(workflowPackage);
    }
    return packages;
  }
}
