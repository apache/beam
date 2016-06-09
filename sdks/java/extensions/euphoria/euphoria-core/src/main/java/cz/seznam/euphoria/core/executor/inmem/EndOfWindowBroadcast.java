package cz.seznam.euphoria.core.executor.inmem;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

abstract class EndOfWindowBroadcast {

  static class NoopInstance extends EndOfWindowBroadcast {
    @Override
    void subscribe(Subscriber s) {
      // ~ no-op
    }
    @Override
    void notifyEndOfWindow(EndOfWindow eow, Subscriber src,
                           Collection<Subscriber> excludes)
    {
      // ~ no-op
    }
  }

  static class NotifyingInstance extends EndOfWindowBroadcast {
    private final List<Subscriber> subscribers = new CopyOnWriteArrayList<>();

    @Override
    void subscribe(Subscriber s) {
      subscribers.add(Objects.requireNonNull(s));
    }

    @Override
    void notifyEndOfWindow(EndOfWindow eow, Subscriber src,
                           Collection<Subscriber> excludes)
    {
      for (Subscriber s : subscribers) {
        if (!src.equals(s) && !excludes.contains(s)) {
          s.onEndOfWindowBroadcast(eow, src);
        }
      }
    }
  }

  interface Subscriber {
    void onEndOfWindowBroadcast(EndOfWindow eow, Subscriber src);
  }

  abstract void subscribe(Subscriber s);

  abstract void notifyEndOfWindow(EndOfWindow eow, Subscriber src,
                                  Collection<Subscriber> excludes);
}
