package org.apache.beam.examples.snippets.transforms.io.webapis;

import org.apache.beam.sdk.coders.ByteArrayCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.vendor.grpc.v1p60p1.com.google.protobuf.ByteString;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ImageResponseCoder extends CustomCoder<ImageResponse> {
    public static ImageResponseCoder of() {
        return new ImageResponseCoder();
    }
    private static final Coder<byte[]> BYTE_ARRAY_CODER = ByteArrayCoder.of();
    private static final Coder<String> STRING_CODER = StringUtf8Coder.of();
    @Override
    public void encode(ImageResponse value, OutputStream outStream) throws CoderException, IOException {
        BYTE_ARRAY_CODER.encode(value.getData().toByteArray(), outStream);
        STRING_CODER.encode(value.getMimeType(), outStream);
    }

    @Override
    public ImageResponse decode(InputStream inStream) throws CoderException, IOException {
        byte[] data = BYTE_ARRAY_CODER.decode(inStream);
        String mimeType = STRING_CODER.decode(inStream);
        return ImageResponse.builder()
                .setData(ByteString.copyFrom(data))
                .setMimeType(mimeType)
                .build();
    }
}
