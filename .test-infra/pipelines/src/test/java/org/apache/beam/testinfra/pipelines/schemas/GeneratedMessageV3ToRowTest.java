package org.apache.beam.testinfra.pipelines.schemas;

import com.google.dataflow.v1beta3.ExecutionStageState;
import com.google.dataflow.v1beta3.Job;
import com.google.dataflow.v1beta3.JobState;
import com.google.dataflow.v1beta3.JobType;
import com.google.dataflow.v1beta3.Step;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Value;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class GeneratedMessageV3ToRowTest {
    @Test
    void build_Job() {
        DescriptorSchemaRegistry registry = new DescriptorSchemaRegistry();
        registry.build(Job.getDescriptor());
        Job job = Job.getDefaultInstance().getDefaultInstanceForType().toBuilder()
                .setId("id_value")
                .setType(JobType.JOB_TYPE_STREAMING)
                .setCreateTime(Timestamp.getDefaultInstance().toBuilder()
                        .setSeconds(1000L)
                        .build())
                .addTempFiles("temp_file_a")
                .addSteps(Step.getDefaultInstance().toBuilder()
                        .setKind("kind_value")
                        .setName("name_value")
                        .setProperties(Struct.getDefaultInstance().toBuilder()
                                .putFields("foo", Value.getDefaultInstance().toBuilder().setStringValue("bar").build())
                                .build())
                        .build())
                .putTransformNameMapping("transform_mapping_key", "transform_mapping_value")
                .addStageStates(ExecutionStageState.getDefaultInstance().toBuilder()
                        .setCurrentStateTime(Timestamp.getDefaultInstance().toBuilder()
                                .setSeconds(1000L)
                                .build())
                        .setExecutionStageName("execution_stage_name_value")
                        .setExecutionStageState(JobState.JOB_STATE_CANCELLING)
                        .build())
                .setSatisfiesPzs(true)
                .build();
        assertNotNull(job);
    }
}