/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.ml.remoteinference.RemoteInference;
import org.apache.beam.sdk.ml.remoteinference.openai.OpenAIModelHandler;
import org.apache.beam.sdk.ml.remoteinference.openai.OpenAIModelInput;
import org.apache.beam.sdk.ml.remoteinference.openai.OpenAIModelParameters;
import org.apache.beam.sdk.ml.remoteinference.openai.OpenAIModelResponse;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptor;

public class Example {
  public static void main(String[] args) {

    /*PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class);
    Pipeline p = Pipeline.create(options);

    p.apply("text", Create.of(
        "An excellent B2B SaaS solution that streamlines business processes efficiently. The platform is user-friendly and highly reliable. Overall, it delivers great value for enterprise teams."))
      .apply(MapElements.into(TypeDescriptor.of(OpenAIModelInput.class))
        .via(OpenAIModelInput::create))
      .apply("inference", RemoteInference.<OpenAIModelInput, OpenAIModelResponse>invoke()
        .handler(OpenAIModelHandler.class)
        .withParameters(OpenAIModelParameters.builder()
          .apiKey("key")
          .modelName("gpt-5-mini")
          .instructionPrompt("Analyse sentiment as positive or negative")
          .build()))
      .apply("print output", ParDo.of(new DoFn<OpenAIModelResponse, Void>() {
        @ProcessElement
        public void print(ProcessContext c) {
          System.out.println("OUTPUT: " + c.element().getOutput());
        }
      }));

    p.run();*/
  }
}
