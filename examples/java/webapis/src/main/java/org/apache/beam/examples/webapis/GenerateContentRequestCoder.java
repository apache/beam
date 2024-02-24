package org.apache.beam.examples.webapis;

import com.google.cloud.vertexai.api.GenerateContentRequest;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class GenerateContentRequestCoder extends CustomCoder<GenerateContentRequest> {

    static GenerateContentRequestCoder of() {
        return new GenerateContentRequestCoder();
    }

    @Override
    public void encode(GenerateContentRequest value, OutputStream outStream) throws CoderException, IOException {
        outStream.write(value.toByteArray());
    }

    @Override
    public GenerateContentRequest decode(InputStream inStream) throws CoderException, IOException {
        return GenerateContentRequest.parseFrom(inStream);
    }
}
