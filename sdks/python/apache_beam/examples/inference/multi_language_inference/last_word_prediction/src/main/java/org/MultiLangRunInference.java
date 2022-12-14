package org;
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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.runners.core.construction.External;
import org.apache.beam.sdk.extensions.python.PythonExternalTransform;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.RowCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.Validation.Required;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.util.ByteStringOutputStream;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.PBegin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiLangRunInference {
    public interface MultiLangueageOptions extends PipelineOptions {

        @Description("Path to an input file that contains labels and pixels to feed into the model")
        @Required
        String getInputFile();

        void setInputFile(String value);

        @Description("Path to a stored model.")
        @Required
        String getModelPath();

        void setModelPath(String value);

        @Description("Path to an input file that contains labels and pixels to feed into the model")
        @Required
        String getOutputFile();

        void setOutputFile(String value);

        @Description("Name of the model on HuggingFace.")
        @Required
        String getModelName();

        void setModelName(String value);

        @Description("Port number of the expansion service.")
        @Required
        String getPort();

        void setPort(String value);
    }

    public static void main(String[] args) {

        MultiLangueageOptions options = PipelineOptionsFactory.fromArgs(args).withValidation()
                .as(MultiLangueageOptions.class);
        
        Pipeline p = Pipeline.create(options);
        PCollection<String> input = p.apply("Read Input", TextIO.read().from(options.getInputFile()));
    
        input.apply("Predict", PythonExternalTransform.<PCollection<String>, PCollection<String>>from(
            "expansion_service.run_inference_expansion.RunInferenceTransform", "localhost:" + options.getPort())
            .withKwarg("model",  options.getModelName())
            .withKwarg("model_path", options.getModelPath()))
            .apply("Write Output", TextIO.write().to(options.getOutputFile()));

        p.run().waitUntilFinish();
    }
}
