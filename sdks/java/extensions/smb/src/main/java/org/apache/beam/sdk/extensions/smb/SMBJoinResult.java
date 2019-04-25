package org.apache.beam.sdk.extensions.smb;

import java.util.Map;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TupleTag;

class SMBJoinResult {
  private final Map<TupleTag, Iterable<?>> valueMap;

  SMBJoinResult(Map<TupleTag, Iterable<?>> valueMap) {
    this.valueMap = valueMap;
  }

  <V> Iterable<V> getValuesForTag(TupleTag<V> tag) {
    return (Iterable<V>) valueMap.get(tag);
  }

  static abstract class ToResult<ResultT> implements SerializableFunction<SMBJoinResult, ResultT> {
    abstract Coder<ResultT> resultCoder();
  }

}
