/**
 * Copyright 2016-2017 Seznam.cz, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.seznam.euphoria.spark;

import cz.seznam.euphoria.core.client.accumulators.AccumulatorProvider;
import cz.seznam.euphoria.core.client.io.Collector;
import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.spark.TaskContext;
import org.apache.spark.TaskContext$;

import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.TransferQueue;

/**
 * Implementation of {@link Collector} using asynchronous {@link TransferQueue}
 * as a workaround to lower the memory footprint
 */
class FunctionCollectorAsync<T> extends FunctionCollector<T> {

  private TransferQueue<Object> queue = new LinkedTransferQueue<>();
  private boolean consumed = false;

  /** Used as an end-of-stream token in the queue */
  private final static Object EOS = new Object();

  /** Used as an end-of-message token in the queue */
  private final static Object EOM = new Object();

  public FunctionCollectorAsync(AccumulatorProvider accumulators) {
    super(accumulators);
  }

  /**
   * Blocks until the item is consumed by runtime.
   */
  @Override
  public void collect(T elem) {
    try {
      queue.transfer(elem);

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
  public Iterator<SparkElement> iterator() {
    if (consumed) {
      // this kind of iterator can be consumed only once
      throw new IllegalStateException("Iterator already consumed");
    }
    consumed = true;

    return new AbstractIterator<SparkElement>() {
      @Override
      protected SparkElement computeNext() {
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
          return (SparkElement) o;
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
    // ~ temporarily transfer spark's thread-local state to the new thread
    TaskContext tctx = TaskContext$.MODULE$.get();
    MEDIATOR_POOL.submit((Runnable) () -> {
      TaskContext$.MODULE$.setTaskContext(tctx);
      try {
        r.run();
        queue.put(EOS);
      } catch (Throwable e) {
        try {
          queue.put(e);
        } catch (InterruptedException e2) {
          Thread.currentThread().interrupt();
        }
      } finally {
        TaskContext$.MODULE$.unset();
      }
    });
  }
}
