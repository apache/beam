package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.dataset.windowing.WindowedElement;
import cz.seznam.euphoria.core.client.io.Context;
import cz.seznam.euphoria.core.client.util.Pair;
import cz.seznam.euphoria.guava.shaded.com.google.common.collect.AbstractIterator;
import cz.seznam.euphoria.guava.shaded.com.google.common.util.concurrent.ThreadFactoryBuilder;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;

/**
 * Implementation of {@link Context} using asynchronous {@link TransferQueue}
 * as a workaround to lower the memory footprint
 */
class FunctionContextAsync<T> extends FunctionContext<T> {

  private TransferQueue<Object> queue = new LinkedTransferQueue<>();
  private boolean consumed = false;

  /** Used as an end-of-stream token in the queue */
  private final static Object EOS = new Object();

  /** Used as an end-of-message token in the queue */
  private final static Object EOM = new Object();

  /**
   * Blocks until the item is consumed by runtime.
   */
  @Override
  public void collect(T elem) {
    try {
      // wrap output to WindowedElement

      // FIXME timestamp set to constant, will be refactored after
      // merging windowing support is added
      WindowedElement wel = new WindowedElement<>(
              window.window(), 0L, Pair.of(window.key(), elem));

      queue.transfer(wel);

      // with every message there is also end-of-message token sent
      // to ensure that the write operation is blocked until the written item
      // is correctly processed (cloned) by the consuming side
      // before another message comes
      queue.transfer(EOM);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  /**
   * Returns blocking iterator.
   */
  public Iterator<WindowedElement> iterator() {
    if (consumed) {
      // this kind of iterator can be consumed only once
      throw new IllegalStateException("Iterator already consumed");
    }
    consumed = true;

    return new AbstractIterator<WindowedElement>() {
      @Override
      protected WindowedElement computeNext() {
        try {
          // this can be item itself, EOS/EOM token or Exception
          Object o = queue.take();

          if (o == EOS) {
            return super.endOfData();
          } else if (o == EOM) {
            // ignore end-of-message token
            return computeNext();
          } else if (o instanceof Throwable) {
            throw (Throwable) o;
          }
          return (WindowedElement) o;
        } catch (Throwable e) {
          throw new RuntimeException(e);
        }
      }
    };
  }

  private static final ExecutorService MEDIATOR_POOL =
          Executors.newCachedThreadPool(
                  new ThreadFactoryBuilder()
                          .setNameFormat("FunctionContext-Mediator-thread-%d")
                          .setDaemon(true)
                          .build());

  /**
   * Run given function in separate thread. Redirect all exception in the
   * {@code queue} and mark end of stream by {@code EOS}
   */
  void runAsynchronously(final Runnable r) {
    MEDIATOR_POOL.submit((Runnable) () -> {
      try {
        r.run();
        queue.put(EOS);
      } catch (Throwable e) {
        try {
          queue.put(e);
        } catch (InterruptedException e2) {
          Thread.currentThread().interrupt();
        }
      }
    });
  }
}
