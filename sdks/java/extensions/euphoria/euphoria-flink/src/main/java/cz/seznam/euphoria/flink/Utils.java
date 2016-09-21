package cz.seznam.euphoria.flink;

import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.core.client.util.Triple;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;

public class Utils {

  @SuppressWarnings("unchecked")
  static class QueryableKeySelector<K, V>
      implements KeySelector<K, V>, ResultTypeQueryable {

    private final KeySelector<K, V> inner;
    private final Class clz;

    QueryableKeySelector(KeySelector<K, V> inner) {
      this(inner, Object.class);
    }

    QueryableKeySelector(KeySelector<K, V> inner, Class clz) {
      this.inner = inner;
      this.clz = clz;
    }


    @Override
    public V getKey(K in) throws Exception {
      return inner.getKey(in);
    }

    @Override
    public TypeInformation getProducedType() {
      return TypeInformation.of(clz);
    }

  }
  
  @SuppressWarnings("unchecked")
  public static <K, V, P extends Pair<K, V>> KeySelector<P, K> keyByPairFirst() {
    return wrapQueryable(Pair::getFirst);
  }

  @SuppressWarnings("unchecked")
  public static <K, V, P extends Pair<K, V>> KeySelector<P, V> keyByPairSecond() {
    return wrapQueryable(Pair::getSecond);
  }

  public static <A, B, C> KeySelector<Triple<A, B, C>, A> keyByTripleFirst() {
    return wrapQueryable(Triple::getFirst);
  }

  public static <A, B, C> KeySelector<Triple<A, B, C>, B> keyByTripleSecond() {
    return wrapQueryable(Triple::getSecond);
  }

  public static <A, B, C> KeySelector<Triple<A, B, C>, C> keyByTripleThird() {
    return wrapQueryable(Triple::getThird);
  }

  public static <K, V> KeySelector<K, V> wrapQueryable(KeySelector<K, V> inner) {
    return new QueryableKeySelector<>(inner);
  }

  public static <K, V> KeySelector<K, V> wrapQueryable(
      KeySelector<K, V> inner, Class<?> clz) {
    return new QueryableKeySelector(inner, clz);
  }

}
