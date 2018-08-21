package org.apache.beam.sdk.extensions.euphoria.core.translate.coder;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Objects;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.extensions.euphoria.core.client.functional.VoidFunction;
import org.objenesis.strategy.StdInstantiatorStrategy;

/**
 * {@link Coder} which encodes/decodes objects of given {@link Class} through {@link Kryo}.
 */
public class ClassAwareKryoCoder<T> extends Coder<T> {

  /**
   * Initial size of byte buffers in {@link Output}, {@link Input}.
   */
  private static final int DEFAULT_BUFFER_SIZE = 4096;

  // this factory needs to be serializable, since the Coder itself is
  private static final VoidFunction<Kryo> KRYO_FACTORY =
      () -> {
        final Kryo instance = new Kryo();
        ((Kryo.DefaultInstantiatorStrategy) instance.getInstantiatorStrategy())
            .setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
        return instance;
      };

  private transient Kryo kryo;
  private transient Output output;
  private transient Input input;

  private Class<T> clazz;

  public ClassAwareKryoCoder(Class<T> clazz) {
    Objects.requireNonNull(clazz);
    this.clazz = clazz;
  }

  public static <T> ClassAwareKryoCoder<T> of(Class<T> clazz) {
    return new ClassAwareKryoCoder<>(clazz);
  }

  @Override
  public synchronized void encode(T value, OutputStream outStream)
      throws CoderException, IOException {
    Output output = getOutput();
    output.setOutputStream(outStream);
    getKryo().writeObjectOrNull(output, value, clazz);
    output.flush();
  }

  @Override
  public synchronized T decode(InputStream inStream) throws CoderException, IOException {
    Input input = getInput();
    input.setInputStream(inStream);
    return getKryo().readObjectOrNull(input, clazz);
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    return null;
  } //TODO Is there a way of knowing this ?

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    //no op
  }

  public Class<T> getClazz() {
    return clazz;
  }

  private Kryo getKryo() {
    if (kryo == null) {
      kryo = KRYO_FACTORY.apply();
    }

    return kryo;
  }

  private Output getOutput() {
    if (output == null) {
      output = new Output(DEFAULT_BUFFER_SIZE, -1);
    }
    return output;
  }

  private Input getInput() {
    if (input == null) {
      input = new Input(DEFAULT_BUFFER_SIZE);
    }

    return input;
  }
}
