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
package org.apache.beam.runners.ignite;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

/** Ignite specific implementation of Beam's {@link PipelineRunner}. */
public class IgniteRunner extends PipelineRunner<PipelineResult> {

    private static final Logger LOG = LoggerFactory.getLogger(IgniteRunner.class);

    public static IgniteRunner fromOptions(
            PipelineOptions options, Function<IgniteConfiguration, Ignite> igniteSupplier) {
        return new IgniteRunner(options, igniteSupplier);
    }

    private final IgnitePipelineOptions options;
    private final Function<IgniteConfiguration, Ignite> igniteSupplier;

    private Function<PTransform<?, ?>, IgniteTransformTranslator<?>> translatorProvider;

    private IgniteRunner(
            PipelineOptions options, Function<IgniteConfiguration, Ignite> igniteSupplier) {
        this.options = validate(options.as(IgnitePipelineOptions.class));
        this.igniteSupplier = igniteSupplier;
        this.translatorProvider = IgniteTransformTranslators::getTranslator;
    }

    @Override
    public PipelineResult run(Pipeline pipeline) {
        return null;
    }
}
