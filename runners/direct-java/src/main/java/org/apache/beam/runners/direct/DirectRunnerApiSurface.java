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
package org.apache.beam.runners.direct;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.metrics.MetricResults;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsRegistrar;
import org.apache.beam.sdk.transforms.display.DisplayData;
import org.apache.beam.sdk.util.ApiSurface;

/**
 * Specialization of {{@link ApiSurface}} that exposes the API surface for Direct Runners.
 */
public class DirectRunnerApiSurface extends ApiSurface {
    protected DirectRunnerApiSurface(Set<Class<?>> rootClasses, Set<Pattern> patternsToPrune) {
        super(rootClasses, patternsToPrune);
    }

    @Override
    public ApiSurface buildApiSurface() throws IOException {
        final Package thisPackage = getClass().getPackage();
        final ClassLoader thisClassLoader = getClass().getClassLoader();
        ApiSurface apiSurface = new DirectRunnerApiSurface(Collections.<Class<?>>emptySet(),
                Collections.<Pattern>emptySet())
                .ofPackage(thisPackage, thisClassLoader)
                .ofPackage(thisPackage, thisClassLoader)
                // Do not include dependencies that are required based on the known exposures. This
                // could alternatively prune everything exposed by the public parts of the Core SDK
                .pruningClass(Pipeline.class)
                .pruningClass(PipelineRunner.class)
                .pruningClass(PipelineOptions.class)
                .pruningClass(PipelineOptionsRegistrar.class)
                .pruningClass(PipelineOptions.DirectRunner.class)
                .pruningClass(DisplayData.Builder.class)
                .pruningClass(MetricResults.class)
                .pruningClass(DirectRunnerApiSurface.class)
                .pruningPattern("org[.]apache[.]beam[.].*Test.*")
                .pruningPattern("org[.]apache[.]beam[.].*IT")
                .pruningPattern("java[.]io.*")
                .pruningPattern("java[.]lang.*")
                .pruningPattern("java[.]util.*");
        return apiSurface;
    }

    @Override
    protected ApiSurface ofRootClassesAndPatternsToPrune(Set<Class<?>> rootClasses,
                                                         Set<Pattern> patternsToPrune) {
        return new DirectRunnerApiSurface(rootClasses, patternsToPrune);
    }
}
