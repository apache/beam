package org.apache.beam.sdk.extensions.euphoria.core.translate.coder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.extensions.euphoria.core.translate.BeamFlow;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * Convenient way of registering Beam {@link Coder} to given {@link Pipeline} or {@link BeamFlow}.
 */
public class RegisterCoders extends CoderProvider {

  private final Map<TypeDescriptor, Coder<?>> typeToCoder;
  private final Map<Class<?>, Coder<?>> classToCoder;

  private RegisterCoders(
      Map<TypeDescriptor, Coder<?>> typeToCoder,
      Map<Class<?>, Coder<?>> classToCoder) {
    this.typeToCoder = typeToCoder;
    this.classToCoder = classToCoder;
  }

  public static RegisterBuilder to(Pipeline pipeline) {
    return new Builder(Objects.requireNonNull(pipeline));
  }

  public static RegisterBuilder to(BeamFlow flow) {
    return to(Objects.requireNonNull(flow).getPipeline());
  }

  @Override
  public <T> Coder<T> coderFor(TypeDescriptor<T> typeDescriptor,
      List<? extends Coder<?>> componentCoders) throws CannotProvideCoderException {

    // try to obtain most specific coder by type descriptor
    Coder<?> coder = typeToCoder.get(typeDescriptor);

    // second try, obtain coder by raw encoding type
    if (coder == null) {
      Class<? super T> rawType = typeDescriptor.getRawType();
      coder = classToCoder.get(rawType);
    }

    if (coder == null) {
      throw new CannotProvideCoderException(String.format(
          "No coder for given type descriptor '%s' found.", typeDescriptor));
    }

    @SuppressWarnings("unchecked")
    Coder<T> castedCoder = (Coder<T>) coder;

    return castedCoder;
  }

  // ----------------------------- builder chain

  /**
   * Builder which defines all the registration methods.
   */
  public interface RegisterBuilder {

    /**
     * Registers custom {@link Coder} for given parametrized {@link TypeDescriptor}.
     *
     * @param type type to register coder for
     * @param coder coder to register
     * @param <T> type of elements encoded by given {@code coder}
     * @return {@link RegisterBuilder} to allow for more coders registration.
     */
    <T> RegisterBuilder regCustomTypedCoder(TypeDescriptor<T> type, Coder<T> coder);

    /**
     * Registers custom {@link Coder} for given raw {@link Class type}.
     *
     * @param clazz type to register coder for
     * @param coder coder to register
     * @param <T> type of elements encoded by given {@code coder}
     * @return {@link RegisterBuilder} to allow for more coders registration.
     */
    <T> RegisterBuilder regCustomRawTypeCoder(Class<T> clazz, Coder<T> coder);

    /**
     * Registers new {@link ClassAwareKryoCoder} for given raw {@link Class type}.
     *
     * @param clazz type to register coder for
     * @param <T> type of elements encoded by given {@code coder}
     * @return {@link RegisterBuilder} to allow for more coders registration.
     */
    <T> RegisterBuilder regKryoRawTypeCoder(Class<T> clazz);

    /**
     * Effectively ends coders registration. No coders registration is done without it.
     */
    void done();
  }

  // ----------------------------- builder itself


  /**
   * Builder of {@link RegisterCoders}.
   */
  public static class Builder implements RegisterBuilder {

    private final Pipeline pipeline;
    private final Map<TypeDescriptor, Coder<?>> typeToCoder = new HashMap<>();
    private final Map<Class<?>, Coder<?>> classToCoder = new HashMap<>();

    public Builder(Pipeline pipeline) {
      this.pipeline = pipeline;
    }

    @Override
    public <T> RegisterBuilder regCustomTypedCoder(TypeDescriptor<T> type, Coder<T> coder) {
      Objects.requireNonNull(type);
      Objects.requireNonNull(coder);
      typeToCoder.put(type, coder);
      return this;
    }

    @Override
    public <T> RegisterBuilder regCustomRawTypeCoder(Class<T> clazz, Coder<T> coder) {
      Objects.requireNonNull(clazz);
      Objects.requireNonNull(coder);
      classToCoder.put(clazz, coder);
      return this;
    }

    @Override
    public <T> RegisterBuilder regKryoRawTypeCoder(Class<T> clazz) {
      Objects.requireNonNull(clazz);
      classToCoder.put(clazz, new ClassAwareKryoCoder<>(clazz));
      return this;
    }

    @Override
    public void done() {
      RegisterCoders registerCoders = new RegisterCoders(typeToCoder, classToCoder);
      pipeline.getCoderRegistry().registerCoderProvider(registerCoders);
    }
  }

}
