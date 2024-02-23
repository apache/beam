package org.apache.beam.examples.snippets.transforms.io.webapis;

import com.google.auto.value.AutoValue;
import com.google.cloud.vertexai.api.GenerateContentRequest;
import com.google.cloud.vertexai.api.GenerateContentResponse;
import com.google.cloud.vertexai.generativeai.GenerativeModel;
import org.apache.beam.io.requestresponse.Caller;
import org.apache.beam.io.requestresponse.SetupTeardown;
import org.apache.beam.io.requestresponse.UserCodeExecutionException;
import com.google.cloud.vertexai.VertexAI;
import org.checkerframework.checker.nullness.qual.MonotonicNonNull;

// [START webapis_gemini_ai_client]

@AutoValue
public abstract class GeminiAIClient implements
        Caller<GenerateContentRequest, GenerateContentResponse>,
        SetupTeardown {

    public static final String MODEL_GEMINI_PRO = "gemini-pro";
    public static final String MODEL_GEMINI_PRO_VISION = "gemini-pro-vision";

    private static final String GOOGLE_CLOUD_PLATFORM_SCOPE =
            "https://www.googleapis.com/auth/cloud-platform";
    private transient @MonotonicNonNull VertexAI vertexAI;
    private transient @MonotonicNonNull GenerativeModel client;

    @Override
    public GenerateContentResponse call(GenerateContentRequest request) throws UserCodeExecutionException {
        return null;
    }

    @Override
    public void setup() throws UserCodeExecutionException {

    }

    @Override
    public void teardown() throws UserCodeExecutionException {

    }

// [END webapis_gemini_ai_client]
}
