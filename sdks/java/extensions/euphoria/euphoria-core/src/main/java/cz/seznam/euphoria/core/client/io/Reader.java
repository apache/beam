package cz.seznam.euphoria.core.client.io;

import java.io.Closeable;
import java.util.Iterator;

/**
 * Reader of data in a particular partition,
 * which essentially is merely a closable iterator.
 */
public interface Reader<E> extends Closeable, Iterator<E> {

}
