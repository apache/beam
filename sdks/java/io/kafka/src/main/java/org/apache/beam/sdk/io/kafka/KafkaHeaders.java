package org.apache.beam.sdk.io.kafka;

/**
 * This is a copy of Kafka's {@link org.apache.kafka.common.header.Headers}. Included here in order
 * to support older Kafka versions (0.9.x).
 */
public interface KafkaHeaders extends Iterable<KafkaHeader> {
  /**
   * Adds a header (key inside), to the end, returning if the operation succeeded.
   *
   * @param header the Header to be added
   * @return this instance of the Headers, once the header is added.
   * @throws IllegalStateException is thrown if headers are in a read-only state.
   */
  KafkaHeaders add(KafkaHeader header) throws IllegalStateException;

  /**
   * Creates and adds a header, to the end, returning if the operation succeeded.
   *
   * @param key of the header to be added.
   * @param value of the header to be added.
   * @return this instance of the Headers, once the header is added.
   * @throws IllegalStateException is thrown if headers are in a read-only state.
   */
  KafkaHeaders add(String key, byte[] value) throws IllegalStateException;

  /**
   * Removes all headers for the given key returning if the operation succeeded.
   *
   * @param key to remove all headers for.
   * @return this instance of the Headers, once the header is removed.
   * @throws IllegalStateException is thrown if headers are in a read-only state.
   */
  KafkaHeaders remove(String key) throws IllegalStateException;

  /**
   * Returns just one (the very last) header for the given key, if present.
   *
   * @param key to get the last header for.
   * @return this last header matching the given key, returns none if not present.
   */
  KafkaHeader lastHeader(String key);

  /**
   * Returns all headers for the given key, in the order they were added in, if present.
   *
   * @param key to return the headers for.
   * @return all headers for the given key, in the order they were added in, if NO headers are
   *     present an empty iterable is returned.
   */
  Iterable<KafkaHeader> headers(String key);

  /**
   * Returns all headers as an array, in the order they were added in.
   *
   * @return the headers as a Header[], mutating this array will not affect the Headers, if NO
   *     headers are present an empty array is returned.
   */
  KafkaHeader[] toArray();
}
