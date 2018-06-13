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
package org.apache.beam.runners.core.construction;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Allows to configure how the staging files are selected.
 * TODO: allow to select in parent classloaders as well as current one.
 */
public interface FilterableStagingFilesPipelineOptions extends PipelineOptions {
    /**
     * The filter to use to select the files from the classloader to keep
     * when staging the classloader.
     * The regex is applied on the file name.
     */
    @Description("Include regex for file staging.")
    String getClassLoaderIncludeFilter();
    void setClassLoaderIncludeFilter(String value);

    /**
     * The filter to use to not select the files from the classloader
     * when staging the classloader.
     * The regex is applied on the file name.
     */
    @Description("Exclude regex for file staging.")
    String getClassLoaderExcludeFilter();
    void setClassLoaderExcludeFilter(String value);
}
