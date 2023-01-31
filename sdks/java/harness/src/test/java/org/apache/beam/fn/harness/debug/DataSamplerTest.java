package org.apache.beam.fn.harness.debug;

import org.apache.beam.fn.harness.PTransformRunnerFactoryTestContext;
import org.apache.beam.model.pipeline.v1.RunnerApi;
import org.apache.beam.runners.core.construction.CoderTranslation;
import org.apache.beam.runners.core.construction.RehydratedComponents;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
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
public class DataSamplerTest {
    @Test
    public void testCreatingAndProcessingWithSampling() throws Exception {
        DataSampler sampler = new DataSampler();

        VarIntCoder coder = VarIntCoder.of();
        sampler.sampleOutput("descriptor-id", "pcollection-id", coder).sample(12345);

        System.out.println(sampler.samples());
    }
}
