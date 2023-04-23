package org.apache.beam.sdk.io.fileschematransform;

import org.apache.beam.model.pipeline.v1.ExternalTransforms;
import org.apache.beam.sdk.expansion.service.ExpansionService;
import org.apache.beam.sdk.expansion.service.ExpansionServiceSchemaTransformProvider;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaTranslation;
import org.apache.beam.vendor.grpc.v1p54p0.com.google.protobuf.InvalidProtocolBufferException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Base64;

import static org.apache.beam.sdk.io.fileschematransform.FileWriteSchemaTransformProviderTest.CONFIGURATION_SCHEMA;
import static org.junit.Assert.*;

/** Tests for {@link FileWriteSchemaTransformProvider} in the context of the {@link ExpansionServiceSchemaTransformProvider}. */
@RunWith(JUnit4.class)
public class FileWriteSchemaTransformExpansionServiceTest {
    private static final String CONFIGURATION_PAYLOAD_BASE64 = "CvwDCgwKBmZvcm1hdBoCEAcKFAoOZmlsZW5hbWVQcmVmaXgaAhAHChEKC2NvbXByZXNzaW9uGgIQBwoPCgludW1TaGFyZHMaAhADChcKEXNoYXJkTmFtZVRlbXBsYXRlGgIQBwoUCg5maWxlbmFtZVN1ZmZpeBoCEAcKWwoQY3N2Q29uZmlndXJhdGlvbhpHCAEyQwpBChkKE3ByZWRlZmluZWRDc3ZGb3JtYXQaAhAHEiQ5Y2FmZDJhOS1iMDIxLTRjZGUtYjNmYi1iY2JlMzgzZTA5YzAKdAoUcGFycXVldENvbmZpZ3VyYXRpb24aXAgBMlgKVgoaChRjb21wcmVzc2lvbkNvZGVjTmFtZRoCEAcKEgoMcm93R3JvdXBTaXplGgIQAxIkNjU2ZGRjN2MtN2M0ZS00YjdmLTllNTktMjk1MDdmNWMyZWVjCmIKEHhtbENvbmZpZ3VyYXRpb24aTggBMkoKSAoRCgtyb290RWxlbWVudBoCEAcKDQoHY2hhcnNldBoCEAcSJGMwYjFmMGMwLWMyMjEtNGQxNy1iNzVlLTNiOGNjMGIyNTlmYhIkZDc0NWExNGYtNTE2Ni00MjhhLTljNTEtNTFiNTgzM2MwNzQ4GiYKGmJlYW06c2NoZW1hOmdvOm5pbGxhYmxlOnYxEgIQCBoECgJAARIgCQLAAQRqc29uEmdzOi8vYnVja2V0L29iamVjdAAAAAA=";

    @Test
    public void testExternalConfigurationPayload() throws InvalidProtocolBufferException {
        ExternalTransforms.ExternalConfigurationPayload configurationPayload = ExternalTransforms.ExternalConfigurationPayload.parseFrom(Base64.getDecoder().decode(CONFIGURATION_PAYLOAD_BASE64));
        Schema payloadSchema = SchemaTranslation.schemaFromProto(configurationPayload.getSchema());
        assertTrue(payloadSchema.assignableTo(CONFIGURATION_SCHEMA));
    }
}