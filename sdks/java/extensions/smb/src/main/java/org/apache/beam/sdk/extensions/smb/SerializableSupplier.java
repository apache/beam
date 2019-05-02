package org.apache.beam.sdk.extensions.smb;

import org.apache.beam.vendor.guava.v20_0.com.google.common.base.Supplier;

import java.io.Serializable;

public interface SerializableSupplier<T> extends Supplier<T>, Serializable {}