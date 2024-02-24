package org.apache.beam.examples.webapis;

import com.google.cloud.vertexai.api.GenerateContentRequest;
import com.google.cloud.vertexai.api.GenerateContentResponse;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class GenerateContentResponseCoder extends CustomCoder<GenerateContentResponse> {

    static GenerateContentResponseCoder of() {
        return new GenerateContentResponseCoder();
    }

    @Override
    public void encode(GenerateContentResponse value, OutputStream outStream) throws CoderException, IOException {
        outStream.write(value.toByteArray());
    }

    @Override
    public GenerateContentResponse decode(InputStream inStream) throws CoderException, IOException {
        return GenerateContentResponse.parseFrom(inStream);
    }
}
