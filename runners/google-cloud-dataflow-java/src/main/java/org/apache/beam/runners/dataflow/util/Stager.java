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
package org.apache.beam.runners.dataflow.util;

import com.google.api.services.dataflow.model.DataflowPackage;
import java.util.List;

/** Interface for staging files needed for running a Dataflow pipeline. */
public interface Stager {
  /**
   * Stage files and return a list of packages {@link DataflowPackage} objects describing th actual
   * location at which each file was staged.
   *
   * <p>The mechanism for staging is owned by the implementation. The only requirement is that the
   * location specified in the returned {@link DataflowPackage} should, in fact, contain the
   * contents of the staged file.
   */
  List<DataflowPackage> stageFiles(List<PackageUtil.StagedFile> filesToStage);

  /** Stage bytes to a target file name wherever this stager stages things. */
  DataflowPackage stageToFile(byte[] bytes, String baseName);
}
