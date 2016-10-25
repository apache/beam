package cz.seznam.euphoria.core.client.operator.state;

import cz.seznam.euphoria.core.client.functional.BinaryFunction;

/**
 * Descriptor of {@code ValueStorage}.
 */
public class ValueStorageDescriptor<T> extends StorageDescriptorBase {

  public static final class MergingValueStorageDescriptor<T>
      extends ValueStorageDescriptor<T>
      implements MergingStorageDescriptor<T> {

    private final BinaryFunction<T, T, T> merger;

    MergingValueStorageDescriptor(
        String name, Class<T> cls, T defVal,
        BinaryFunction<T, T, T> merger) {
      super(name, cls, defVal);
      this.merger = merger;
    }

    @Override
    public BinaryFunction<ValueStorage<T>, ValueStorage<T>, Void> getMerger() {
      return (l, r) -> {
        l.set(getValueMerger().apply(l.get(), r.get()));
        return null;
      };
    }

    public BinaryFunction<T, T, T> getValueMerger() {
      return merger;
    }
  }

  /** Get descriptor of value storage without merging. */
  public static <T> ValueStorageDescriptor<T> of(String name, Class<T> cls, T defVal) {
    return new ValueStorageDescriptor<>(name, cls, defVal);
  }

  /**
   * Get mergeable value storage descriptor.
   * This is needed in conjunction with all merging windowings
   * and for all state storages.
   */
  public static <T> ValueStorageDescriptor<T> of(
      String name, Class<T> cls, T defVal, BinaryFunction<T, T, T> merger) {
    return new MergingValueStorageDescriptor<>(name, cls, defVal, merger);
  }

  private final Class<T> cls;
  private final T defVal;

  private ValueStorageDescriptor(String name, Class<T> cls, T defVal) {
    super(name);
    this.cls = cls;
    this.defVal = defVal;
  }  
  
  public Class<T> getValueClass() { return cls; }
  
  public T getDefaultValue() { return defVal; }


}
