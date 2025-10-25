import org.apache.beam.runners.direct.DirectRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.ml.remoteinference.RemoteInference;
import org.apache.beam.sdk.ml.remoteinference.openai.OpenAiModelHandler;
import org.apache.beam.sdk.ml.remoteinference.openai.OpenAiModelInput;
import org.apache.beam.sdk.ml.remoteinference.openai.OpenAiModelParameters;
import org.apache.beam.sdk.ml.remoteinference.openai.OpenAiModelResponse;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptor;

public class Example {
  public static void main(String[] args) {

    PipelineOptions options = PipelineOptionsFactory.create();
    options.setRunner(DirectRunner.class);
    Pipeline p = Pipeline.create(options);

    p.apply("text", Create.of(
        "An excellent B2B SaaS solution that streamlines business processes efficiently. The platform is user-friendly and highly reliable. Overall, it delivers great value for enterprise teams."))
      .apply(MapElements.into(TypeDescriptor.of(OpenAiModelInput.class))
        .via(OpenAiModelInput::create))
      .apply("inference", RemoteInference.<OpenAiModelInput, OpenAiModelResponse>invoke()
        .handler(OpenAiModelHandler.class)
        .withParameters(OpenAiModelParameters.builder()
          .apiKey("key")
          .modelName("gpt-5-mini")
          .instructionPrompt("Analyse sentiment as positive or negative")
          .build()))
      .apply("print output", ParDo.of(new DoFn<OpenAiModelResponse, Void>() {
        @ProcessElement
        public void print(ProcessContext c) {
          System.out.println("OUTPUT: " + c.element().getOutput());
        }
      }));

    p.run();
  }
}
