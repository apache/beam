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
package org.apache.beam.sdk.extensions.euphoria.core.translate;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.beam.sdk.extensions.euphoria.core.client.accumulators.AccumulatorProvider;
import org.apache.beam.sdk.extensions.euphoria.core.translate.provider.GenericTranslatorProvider;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Pipeline options related to Euphoria DSL translation.
 *
 * @deprecated Use Java SDK directly, Euphoria is scheduled for removal in a future release.
 */
@Deprecated
public interface EuphoriaOptions extends PipelineOptions {

  /** {@link DefaultValueFactory} of {@link TranslatorProvider}. */
  class DefaultTranslatorProviderFactory implements DefaultValueFactory<TranslatorProvider> {

    @Override
    public TranslatorProvider create(PipelineOptions options) {
      return GenericTranslatorProvider.createWithDefaultTranslators();
    }
  }

  /** {@link DefaultValueFactory} of {@link AccumulatorProvider.Factory}. */
  class DefaultAccumulatorProviderFactory
      implements DefaultValueFactory<AccumulatorProvider.Factory> {

    @Override
    public AccumulatorProvider.Factory create(PipelineOptions options) {
      return BeamAccumulatorProvider.Factory.get();
    }
  }

  @Description("Euphoria translation provider")
  @Default.InstanceFactory(DefaultTranslatorProviderFactory.class)
  @JsonIgnore
  TranslatorProvider getTranslatorProvider();

  void setTranslatorProvider(TranslatorProvider translationProvider);

  @Description("Euphoria accumulator provider factory")
  @Default.InstanceFactory(DefaultAccumulatorProviderFactory.class)
  @JsonIgnore
  AccumulatorProvider.Factory getAccumulatorProviderFactory();

  void setAccumulatorProviderFactory(AccumulatorProvider.Factory accumulatorProviderFactory);
}
