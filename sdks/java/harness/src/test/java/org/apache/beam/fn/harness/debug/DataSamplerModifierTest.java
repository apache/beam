package org.apache.beam.fn.harness.debug;

import org.apache.beam.fn.harness.PTransformRunnerFactoryTestContext;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.ByteArrayOutputStream;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.beam.sdk.util.WindowedValue.valueInGlobalWindow;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

@RunWith(JUnit4.class)
public class DataSamplerModifierTest {

    @Test
    public void testCreatingAndProcessingWithSampling() throws Exception {
        // Create the DataSampling PTransform.
        String pTransformId = "pTransformId";

        RunnerApi.FunctionSpec functionSpec =
                RunnerApi.FunctionSpec.newBuilder()
                        .setUrn(DataSamplingFnRunner.URN)
                        .build();
        RunnerApi.PTransform pTransform =
                RunnerApi.PTransform.newBuilder()
                        .setSpec(functionSpec)
                        .putInputs("input", "inputTarget")
                        .build();

        // Populate fake input PCollections.
        Map<String, RunnerApi.PCollection> pCollectionMap = new HashMap<>();
        pCollectionMap.put(
                "inputTarget",
                RunnerApi.PCollection.newBuilder()
                        .setUniqueName("inputTarget")
                        .setCoderId("coder-id")
                        .build());

        // Populate the PTransform context that includes the DataSampler.
        DataSampler dataSampler = new DataSampler();
        RunnerApi.Coder coder = CoderTranslation.toProto(StringUtf8Coder.of()).getCoder();
        PTransformRunnerFactoryTestContext context =
                PTransformRunnerFactoryTestContext.builder(pTransformId, pTransform)
                        .processBundleInstructionId("instruction-id")
                        .pCollections(pCollectionMap)
                        .coders(Collections.singletonMap("coder-id", coder))
                        .dataSampler(dataSampler)
                        .build();

        // Create the runner which samples the input PCollection.
        new DataSamplingFnRunner.Factory<>().createRunnerForPTransform(context);
        assertThat(
                context.getPCollectionConsumers().keySet(),
                contains("inputTarget"));

        // Send in a test value that should be sampled.
        context.getPCollectionConsumer("inputTarget").accept(valueInGlobalWindow("Hello, World!"));

        // Rehydrate the given utf-8 string coder.
        RehydratedComponents rehydratedComponents =
                RehydratedComponents.forComponents(
                                RunnerApi.Components.newBuilder()
                                        .putAllCoders(context.getCoders())
                                        .putAllPcollections(context.getPCollections())
                                        .putAllWindowingStrategies(context.getWindowingStrategies())
                                        .build())
                        .withPipeline(Pipeline.create());
        Coder<String> rehydratedCoder = (Coder<String>)rehydratedComponents.getCoder("coder-id");

        Map<String, List<byte[]>> samples = dataSampler.samples();
        assertThat(samples.keySet(), contains("inputTarget"));

        // Ensure that the value was sampled.
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        rehydratedCoder.encode("Hello, World!", outputStream);
        byte[] encodedValue = outputStream.toByteArray();
        assertThat(samples.get("inputTarget"), contains(encodedValue));
    }
}
