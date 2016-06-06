package cz.seznam.euphoria.core.executor.inmem;

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
    void notifyEndOfWindow(EndOfWindow eow, Subscriber excludeSubscriber) {
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
    void notifyEndOfWindow(EndOfWindow eow, Subscriber excludeSubscriber) {
      for (Subscriber s : subscribers) {
        if (!s.equals(excludeSubscriber)) {
          s.onEndOfWindowBroadcast(eow);
        }
      }
    }
  }

  interface Subscriber {
    void onEndOfWindowBroadcast(EndOfWindow eow);
  }

  abstract void subscribe(Subscriber s);

  abstract void notifyEndOfWindow(EndOfWindow eow, Subscriber excludeSubscriber);
}
