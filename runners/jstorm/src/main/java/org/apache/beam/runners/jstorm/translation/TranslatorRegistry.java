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
package org.apache.beam.runners.jstorm.translation;

import com.alibaba.jstorm.beam.translation.translator.*;
import org.apache.beam.runners.jstorm.translation.translator.BoundedSourceTranslator;
import org.apache.beam.runners.jstorm.translation.translator.FlattenTranslator;
import org.apache.beam.runners.jstorm.translation.translator.GroupByKeyTranslator;
import org.apache.beam.runners.jstorm.translation.translator.ParDoBoundMultiTranslator;
import org.apache.beam.runners.jstorm.translation.translator.ParDoBoundTranslator;
import org.apache.beam.runners.jstorm.translation.translator.TransformTranslator;
import org.apache.beam.runners.jstorm.translation.translator.UnboundedSourceTranslator;
import org.apache.beam.runners.jstorm.translation.translator.ViewTranslator;
import org.apache.beam.runners.jstorm.translation.translator.WindowAssignTranslator;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Lookup table mapping PTransform types to associated TransformTranslator implementations.
 */
public class TranslatorRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(TranslatorRegistry.class);

    private static final Map<Class<? extends PTransform>, TransformTranslator> TRANSLATORS = new HashMap<>();

    static {
        TRANSLATORS.put(Read.Bounded.class, new BoundedSourceTranslator());
        TRANSLATORS.put(Read.Unbounded.class, new UnboundedSourceTranslator());
        // TRANSLATORS.put(Write.Bound.class, new WriteSinkStreamingTranslator());
        // TRANSLATORS.put(TextIO.Write.Bound.class, new TextIOWriteBoundStreamingTranslator());

        TRANSLATORS.put(ParDo.SingleOutput.class, new ParDoBoundTranslator());
        TRANSLATORS.put(ParDo.MultiOutput.class, new ParDoBoundMultiTranslator());

        //TRANSLATORS.put(Window.Bound.class, new WindowBoundTranslator<>());
        TRANSLATORS.put(Window.Assign.class, new WindowAssignTranslator<>());

        TRANSLATORS.put(Flatten.PCollections.class, new FlattenTranslator());

        TRANSLATORS.put(GroupByKey.class, new GroupByKeyTranslator());

        TRANSLATORS.put(ViewTranslator.CreateJStormPCollectionView.class, new ViewTranslator());

        /**
         * Currently, empty translation is required for combine and reshuffle. Because, the transforms will be 
         * mapped to GroupByKey and Pardo finally. So we only need to translator the finally transforms.
         * If any improvement is required, the composite transforms will be translated in the future.
         */
        // TRANSLATORS.put(Combine.PerKey.class, new CombinePerKeyTranslator());
        // TRANSLATORS.put(Globally.class, new CombineGloballyTranslator());
        // TRANSLATORS.put(Reshuffle.class, new ReshuffleTranslator());
    }

    public static TransformTranslator<?> getTranslator(PTransform<?, ?> transform) {
        TransformTranslator<?> translator = TRANSLATORS.get(transform.getClass());
        if (translator == null) {
            LOG.warn("Unsupported operator={}", transform.getClass().getName());
        }
        return translator;
    }
}
