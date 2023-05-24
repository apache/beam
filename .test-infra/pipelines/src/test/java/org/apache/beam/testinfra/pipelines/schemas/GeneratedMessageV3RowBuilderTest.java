package org.apache.beam.testinfra.pipelines.schemas;

import com.google.dataflow.v1beta3.Job;
import org.apache.beam.sdk.values.Row;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class GeneratedMessageV3RowBuilderTest {
    @Test
    void build_Job() throws IOException {
        DescriptorSchemaRegistry schemaRegistry = new DescriptorSchemaRegistry();
        schemaRegistry.build(Job.getDescriptor());
        String jobData = loadResource("jobs_data/job1");
        byte[] bytes = Base64.getDecoder().decode(jobData);
        Job job = Job.parseFrom(bytes);
        GeneratedMessageV3RowBuilder<Job> messageV3RowBuilder = new GeneratedMessageV3RowBuilder<>(
                schemaRegistry,
                job
        );
        Row row = messageV3RowBuilder.build();
        assertNotNull(row);
    }

    private static String loadResource(String resourceName) throws IOException {
        Path resourcePath = Paths.get("build", "resources", "test", resourceName);
        byte[] bytes = Files.readAllBytes(resourcePath);
        return new String(bytes, StandardCharsets.UTF_8);
    }
}