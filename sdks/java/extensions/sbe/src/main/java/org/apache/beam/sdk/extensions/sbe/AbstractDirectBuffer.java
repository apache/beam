package org.apache.beam.sdk.extensions.sbe;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.errorprone.annotations.DoNotCall;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import javax.annotation.Nonnull;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.CharMatcher;

/**
 * A default implementation for {@link DirectBuffer} that is designed for use in Beam types.
 *
 * <p> Everything that {@link AbstractDirectBuffer} does is considered safe for PCollections. It
 * creates its own copy of the data and does not allow direct access to that data. For this reason,
 * the following methods are not supported:
 *
 * <ul>
 *   <li>Any {@code wrap} method.
 *   <li>{@link DirectBuffer#addressOffset()}
 *   <li>{@link DirectBuffer#byteArray()}
 *   <li>{@link DirectBuffer#byteBuffer()}
 *   <li>{@link DirectBuffer#wrapAdjustment()}
 * </ul>
 *
 * <p> The last two can still be gotten via copy rather than by accessing the underlying buffer
 * directly.
 *
 * <p> Implementations should use {@link AbstractDirectBuffer#DEFAULT_BYTE_ORDER} to determine the
 * order of bytes. If a passed-in {@link ByteOrder} is different, then the bytes should be reversed
 * before writes or after reads.
 *
 * <p> This class is thread-safe only in that it is read-only. Implementations that allow modifying
 * data will need to override its methods to make them thread-safe. Note that by allowing these
 * modifications, the implementation will no longer be inherently safe for a PCollection.
 *
 * <p> Implementations should be sure to implement {@link Object#equals(Object)} and
 * {@link Object#hashCode()}. Implementations are required to implement
 * {@link Comparable#compareTo(Object)}.
 */
abstract class AbstractDirectBuffer implements DirectBuffer {

  /**
   * Indicates how to handle the passed-in buffer.
   *
   * <p> {@link CreateMode#COPY} will copy the passed-in buffer to one owned by this instance. This
   * should be the preferred method when the source of the data comes from a mutable, external
   * source.
   *
   * <p> {@link CreateMode#VIEW} will use the passed-in buffer as its own backing buffer. This is
   * less safe but can provide a (likely mild) performance improvement if the input data is
   * immutable or if a copy was already made.
   */
  protected enum CreateMode {
    COPY,
    VIEW
  };

  protected static final String UNSAFE_FOR_PCOLLECTION = "Unsafe for PCollection";

  /** Order of bytes in {@link AbstractDirectBuffer#buffer}. */
  protected static final ByteOrder DEFAULT_BYTE_ORDER = ByteOrder.BIG_ENDIAN;

  @Nonnull protected ByteBuffer buffer;
  protected int offset;
  protected int length;

  /**
   * Creates a new instance that handles {@code buffer} according to {@code mode}.
   *
   * <p> If {@code mode} is {@link CreateMode#COPY}, then the underlying buffer may not preserve
   * the data outside the range [position, limit) in relation to the buffer. This data will be
   * preserved if {@code mode} is {@link CreateMode#VIEW}, but only the data in the range
   * [position, limit) will be readable.
   *
   * @param buffer the {@link ByteBuffer} to use to set the underlying data
   * @param mode whether to copy or view {@code buffer}
   */
  protected AbstractDirectBuffer(@Nonnull ByteBuffer buffer, CreateMode mode) {
    if (mode == CreateMode.COPY) {
      this.buffer = createCopyOfByteBuffer(buffer);
      this.offset = 0;
      this.length = this.buffer.limit();
    } else {
      this.buffer = buffer;
      this.offset = buffer.position();
      this.length = buffer.limit() - buffer.position();
    }
  }

  @Override
  @DoNotCall("Always throws UnsupportedOperationException")
  public final void wrap(byte[] buffer) {
    throw new UnsupportedOperationException(createDoesNotSupportMethodMessage("wrap(byte[])", UNSAFE_FOR_PCOLLECTION));
  }

  @Override
  @DoNotCall("Always throws UnsupportedOperationException")
  public final void wrap(byte[] buffer, int offset, int length) {
    throw new UnsupportedOperationException(createDoesNotSupportMethodMessage("wrap(byte[], int, int)", UNSAFE_FOR_PCOLLECTION));
  }

  @Override
  @DoNotCall("Always throws UnsupportedOperationException")
  public final void wrap(ByteBuffer buffer) {
    throw new UnsupportedOperationException(createDoesNotSupportMethodMessage("wrap(ByteBuffer)", UNSAFE_FOR_PCOLLECTION));
  }

  @Override
  @DoNotCall("Always throws UnsupportedOperationException")
  public final void wrap(ByteBuffer buffer, int offset, int length) {
    throw new UnsupportedOperationException(createDoesNotSupportMethodMessage("wrap(ByteBuffer, int, int)", UNSAFE_FOR_PCOLLECTION));
  }

  @Override
  @DoNotCall("Always throws UnsupportedOperationException")
  public final void wrap(DirectBuffer buffer) {
    throw new UnsupportedOperationException(createDoesNotSupportMethodMessage("wrap(DirectBuffer)", UNSAFE_FOR_PCOLLECTION));
  }

  @Override
  @DoNotCall("Always throws UnsupportedOperationException")
  public final void wrap(DirectBuffer buffer, int offset, int length) {
    throw new UnsupportedOperationException(createDoesNotSupportMethodMessage("wrap(DirectBuffer, int, int)", UNSAFE_FOR_PCOLLECTION));
  }

  @Override
  @DoNotCall("Always throws UnsupportedOperationException")
  public final void wrap(long address, int length) {
    throw new UnsupportedOperationException(createDoesNotSupportMethodMessage("wrap(long, int)", UNSAFE_FOR_PCOLLECTION));
  }

  @Override
  @DoNotCall("Always throws UnsupportedOperationException")
  public final long addressOffset() {
    throw new UnsupportedOperationException(createDoesNotSupportMethodMessage("addressOffset()", UNSAFE_FOR_PCOLLECTION));
  }

  @Override
  @DoNotCall("Always throws UnsupportedOperationException - Use getCopyAsArray")
  public final byte[] byteArray() {
    throw new UnsupportedOperationException(createDoesNotSupportMethodMessage("byteArray()", UNSAFE_FOR_PCOLLECTION));
  }

  /**
   * Gets a copy of the underlying buffer as a byte[].
   *
   * @return a byte[] that contains a copy of the data
   */
  public final byte[] getCopyAsArray() {
    byte[] copy = new byte[length];
    getBytes(0, copy);
    return copy;
  }

  @Override
  @DoNotCall("Always throws UnsupportedOperationException - Use getCopyAsBuffer")
  public final ByteBuffer byteBuffer() {
    throw new UnsupportedOperationException(createDoesNotSupportMethodMessage("byteBuffer()", UNSAFE_FOR_PCOLLECTION));
  }

  /**
   * Gets a copy of the underlying buffer as a {@link ByteBuffer}.
   *
   * <p> This will have the same limit, capacity, and position as the underlying buffer. Data from
   * [0, limit) will also be the same, but data from [limit, capacity) may not.
   *
   * @return a {@link ByteBuffer} with a copy of the data
   */
  public final ByteBuffer getCopyAsBuffer() {
    return createCopyOfByteBuffer(buffer);
  }

  /**
   * Creates a copy of {@code buffer}.
   *
   * <p> The returned {@link ByteBuffer} will have the same capacity as {@code buffer}, but it
   * will only contain the data in range
   * [{@link ByteBuffer#position()}, {@link ByteBuffer#limit()}), and the data will start at
   * absolute position zero. In other words, the data in ranges
   * [0, {@link ByteBuffer#position()}) and
   * [{@link ByteBuffer#limit()}, {@link ByteBuffer#capacity()}) will be lost.
   *
   * <p> The returned {@link ByteBuffer} buffer will always be direct.
   *
   * @param buffer {@link ByteBuffer} to copy
   * @return a new {@link ByteBuffer} with the data as specified above
   */
  private static ByteBuffer createCopyOfByteBuffer(@Nonnull ByteBuffer buffer) {
    ByteBuffer copy = ByteBuffer.allocateDirect(buffer.capacity());

    int offset = buffer.position();
    int length = buffer.limit() - offset;
    for (int i = 0; i < length; ++i) {
      copy.put(i, buffer.get(offset + i));
    }
    copy.limit(length);

    return copy;
  }

  @Override
  public int capacity() {
    return buffer.capacity();
  }

  /**
   * Checks that {@code limit} is in range [0, capacity].
   *
   * @param limit value to check
   * @throws IndexOutOfBoundsException if limit is less than zero or greater than capacity
   */
  @Override
  public void checkLimit(int limit) {
    if (limit < 0) {
      throw new IndexOutOfBoundsException(String.format("limit (%s) < 0", limit));
    }

    int capacity = capacity();
    if (limit > capacity()) {
      throw new IndexOutOfBoundsException(String.format("limit (%s) > capacity (%s)", limit, capacity));
    }
  }

  @Override
  public long getLong(int index, ByteOrder byteOrder) {
    long value = buffer.getLong(getInternalIndex(index));
    return byteOrder == DEFAULT_BYTE_ORDER ? value : Long.reverseBytes(value);
  }

  @Override
  public long getLong(int index) {
    return getLong(index, DEFAULT_BYTE_ORDER);
  }

  @Override
  public int getInt(int index, ByteOrder byteOrder) {
    int value = buffer.getInt(getInternalIndex(index));
    return byteOrder == DEFAULT_BYTE_ORDER ? value : Integer.reverseBytes(value);
  }

  @Override
  public int getInt(int index) {
    return getInt(index, DEFAULT_BYTE_ORDER);
  }

  // Constants to help with parsing integers
  private static final char MINUS_SIGN = '-';

  @Override
  public int parseNaturalIntAscii(int index, int length) {
    return Math.toIntExact(parseNaturalLongAscii(index, length));
  }

  @Override
  public long parseNaturalLongAscii(int index, int length) {
    long value = parseLongAscii(index, length);
    if (value < 0) {
      throw new NumberFormatException(String.format("Value (%s) at index %s is not a natural number.", value, index));
    }
    return value;
  }

  @Override
  public int parseIntAscii(int index, int length) {
    return Math.toIntExact(parseLongAscii(index, length));
  }

  @Override
  public long parseLongAscii(int index, int length) {
    boundsCheck(index, length);

    int start = getInternalIndex(index);
    int end = start + length;

    char first = (char) buffer.get(start);  // Since this is ASCII, only grab one byte
    boolean isNegative = first == MINUS_SIGN;
    int preRead;  // How many characters were read to set initial value
    long value;
    if (isNegative) {
      checkArgument(length >= 2, "String represents a negative number. Length must be at least 2 but is %s", length);
      value = -parseCharToLong((char) buffer.get(start + 1));
      preRead = 2;
    } else {
      value = parseCharToLong(first);
      preRead = 1;
    }

    // We could read in the whole string and then parse it, but this allows parsing on a single pass
    String overflowErrorMessage = "Will overflow on index %s";
    String underflowErrorMessage = "Will underflow on index %s";
    for (int i = start + preRead; i < end; ++i) {
      // TODO(zhoufek): Switch to using Math methods when fully on Java 11
      if (!isNegative && value > Long.MAX_VALUE / 10) {
        throw new ArithmeticException(String.format(overflowErrorMessage, i));
      }
      if (isNegative && value < Long.MIN_VALUE / 10) {
        throw new ArithmeticException(String.format(underflowErrorMessage, i));
      }
      value *= 10;

      long asLong = parseCharToLong((char) buffer.get(i));
      if (!isNegative && value > Long.MAX_VALUE - asLong) {
        throw new ArithmeticException(String.format(overflowErrorMessage, i));
      }
      if (isNegative && value < Long.MIN_VALUE + asLong) {
        throw new ArithmeticException(String.format(underflowErrorMessage, i));
      }
      value = isNegative ? value - asLong : value + asLong;
    }

    return value;
  }

  /** Handles parsing a single char value. */
  private static long parseCharToLong(char c) {
    return (long) c - '0';
  }

  @Override
  public double getDouble(int index, ByteOrder byteOrder) {
    return Double.longBitsToDouble(getLong(index, byteOrder));
  }

  @Override
  public double getDouble(int index) {
    return getDouble(index, DEFAULT_BYTE_ORDER);
  }

  @Override
  public float getFloat(int index, ByteOrder byteOrder) {
    return Float.intBitsToFloat(getInt(index, byteOrder));
  }

  @Override
  public float getFloat(int index) {
    return getFloat(index, DEFAULT_BYTE_ORDER);
  }

  @Override
  public short getShort(int index, ByteOrder byteOrder) {
    short value = buffer.getShort(getInternalIndex(index));
    return byteOrder == DEFAULT_BYTE_ORDER ? value : Short.reverseBytes(value);
  }

  @Override
  public short getShort(int index) {
    return getShort(index, DEFAULT_BYTE_ORDER);
  }

  @Override
  public char getChar(int index, ByteOrder byteOrder) {
    char value = buffer.getChar(getInternalIndex(index));
    return byteOrder == DEFAULT_BYTE_ORDER ? value : Character.reverseBytes(value);
  }

  @Override
  public char getChar(int index) {
    return getChar(index, DEFAULT_BYTE_ORDER);
  }

  @Override
  public byte getByte(int index) {
    return buffer.get(getInternalIndex(index));
  }

  @Override
  public void getBytes(int index, byte[] dst) {
    getBytes(index, dst, 0, dst.length);
  }

  @Override
  public void getBytes(int index, byte[] dst, int offset, int length) {
    int start = getInternalIndex(index);
    int end = start + length;

    for (int i = start, pos = offset; i < end; ++i, ++pos) {
      dst[pos] = buffer.get(i);
    }
  }

  @Override
  public void getBytes(int index, MutableDirectBuffer dstBuffer, int dstIndex, int length) {
    int start  = getInternalIndex(index);
    int end = start + length;

    for (int i = start, pos = dstIndex; i < end; ++i, ++pos) {
      dstBuffer.putByte(pos, buffer.get(i));
    }
  }

  @Override
  public void getBytes(int index, ByteBuffer dstBuffer, int length) {
    getBytes(index, dstBuffer, 0, length);
  }

  @Override
  public void getBytes(int index, ByteBuffer dstBuffer, int dstOffset, int length) {
    int start = getInternalIndex(index);
    int end = start + length;

    for (int i = start, pos = dstOffset; i < end; ++i, ++pos) {
      dstBuffer.put(pos, buffer.get(i));
    }
  }

  @Override
  public String getStringAscii(int index) {
    return getStringAscii(index, DEFAULT_BYTE_ORDER);
  }

  @Override
  public int getStringAscii(int index, Appendable appendable) {
    return getStringAscii(index, appendable, DEFAULT_BYTE_ORDER);
  }

  @Override
  public String getStringAscii(int index, ByteOrder byteOrder) {
    return getStringWithoutLengthAscii(index + Integer.BYTES, getInt(index, byteOrder));
  }

  @Override
  public int getStringAscii(int index, Appendable appendable, ByteOrder byteOrder) {
    return getStringWithoutLengthAscii(index + Integer.BYTES, getInt(index, byteOrder), appendable);
  }

  @Override
  public String getStringAscii(int index, int length) {
    return getStringWithoutLengthAscii(index + Integer.BYTES, length);
  }

  @Override
  public int getStringAscii(int index, int length, Appendable appendable) {
    return getStringWithoutLengthAscii(index + Integer.BYTES, length, appendable);
  }

  @Override
  public String getStringWithoutLengthAscii(int index, int length) {
    return getStringWithoutLength(index, length, US_ASCII);
  }

  @Override
  public int getStringWithoutLengthAscii(int index, int length, Appendable appendable) {
    // Though this requires an additional pass, it avoids dirtying the appendable.
    String str = getStringWithoutLengthAscii(index, length);
    try {
      appendable.append(str);
    } catch (IOException e) {
      throw new RuntimeException("Could not append string: ", e);
    }
    return length;
  }

  @Override
  public String getStringUtf8(int index) {
    return getStringUtf8(index, DEFAULT_BYTE_ORDER);
  }

  @Override
  public String getStringUtf8(int index, ByteOrder byteOrder) {
    return getStringWithoutLengthUtf8(index + Integer.BYTES, getInt(index, byteOrder));
  }

  @Override
  public String getStringUtf8(int index, int length) {
    return getStringWithoutLengthUtf8(index + Integer.BYTES, length);
  }

  @Override
  public String getStringWithoutLengthUtf8(int index, int length) {
    return getStringWithoutLength(index, length, UTF_8);
  }

  /**
   * Handles reading a string that isn't length-prefixed from the buffer.
   *
   * @param index index to start read relative to {@link AbstractDirectBuffer#offset}
   * @param length number of characters to read
   * @param charset {@link Charset} indicating which encoding the string is
   * @return the string
   * @throws IllegalArgumentException if {@code charset} is not ASCII or UTF-8
   */
  private String getStringWithoutLength(int index, int length, Charset charset) {
    checkArgument(charset.equals(US_ASCII) || charset.equals(UTF_8),
        "Invalid charset (%s). This only supports ASCII and UTF-8", charset);

    byte[] rawBytes = new byte[length];
    getBytes(index, rawBytes);
    return new String(rawBytes, charset);
  }

  @Override
  public void boundsCheck(int index, int length) {
    if (index < 0) {
      throw new IndexOutOfBoundsException(String.format("Index (%s) is negative", index));
    }
    if (index >= this.length) {
      throw new IndexOutOfBoundsException(String.format("Index (%s) must be less than length of buffer (%s)", index, this.length));
    }
    if (length < 0) {
      throw new IndexOutOfBoundsException(String.format("Length (%s) is negative", length));
    }

    int internalIndex = getInternalIndex(index);
    int end = internalIndex + length;
    int maxAllowedLength = this.length - this.offset;
    if (end > maxAllowedLength) {
      throw new IndexOutOfBoundsException(
          String.format("Length (%s) is too long from index (%s). Max allowed from index: %s", length, index, maxAllowedLength));
    }
  }

  @Override
  @DoNotCall("Always throws UnsupportedOperationException")
  public final int wrapAdjustment() {
    throw new UnsupportedOperationException(createDoesNotSupportMethodMessage("wrapAdjustment()", UNSAFE_FOR_PCOLLECTION));
  }

  @Override
  public abstract int compareTo(DirectBuffer o);

  /**
   * Calculates the actual starting position of a read or write.
   *
   * <p> The value may or may not be equal to {@code index}.
   *
   * @param index index provided by user
   * @return the index to start reads and writes from
   */
  protected int getInternalIndex(int index) {
    return offset + index;
  }

  /**
   * Returns a message describing what method is not supported and why.
   *
   * <p> The format of the message will be
   *
   * <pre>
   *   {@code this.getClass().getName()}#{@code methodDescription} is not supported - {@code reason}
   * </pre>
   *
   * @param methodDescription method description, which ideally will be written like "methodName(T1, T2, ..., TN)"
   * @param reason reason this is not supported
   * @return the complete message
   */
  protected String createDoesNotSupportMethodMessage(String methodDescription, String reason) {
    return String.format("%s#%s is not supported - %s", this.getClass().getName(), methodDescription, reason);
  }
}
