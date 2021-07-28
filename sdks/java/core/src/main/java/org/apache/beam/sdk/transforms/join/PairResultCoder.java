package org.apache.beam.sdk.transforms.join;

import avro.shaded.com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StructuredCoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class PairResultCoder<V1, V2>
    extends StructuredCoder<PairResult<V1, V2>> {
  private final Coder<V1> firstCoder;
  private final Coder<V2> secondCoder;

  private PairResultCoder(
      Coder<V1> firstCoder,
      Coder<V2> secondCoder) {
    this.firstCoder = firstCoder;
    this.secondCoder = secondCoder;
  }

  public static <V1, V2> PairResultCoder<V1, V2> of(
      Coder<V1> firstCoder, Coder<V2> secondCoder) {
    return new PairResultCoder<>(firstCoder, secondCoder);
  }

  @Override
  public void encode(PairResult<V1, V2> value, OutputStream outStream)
      throws CoderException, IOException {
    firstCoder.encode(value.getFirst(), outStream);
    secondCoder.encode(value.getSecond(), outStream);
  }

  @Override
  public PairResult<V1, V2> decode(InputStream inStream) throws CoderException, IOException {
    return PairResult.of(firstCoder.decode(inStream), secondCoder.decode(inStream));
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return ImmutableList.of(firstCoder, secondCoder);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    firstCoder.verifyDeterministic();
    secondCoder.verifyDeterministic();
  }
}
