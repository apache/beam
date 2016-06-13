package cz.seznam.euphoria.core.executor.inmem;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

interface EndOfWindowBroadcast {

  class NoopInstance implements EndOfWindowBroadcast {
    @Override
    public void subscribe(Subscriber s) {
      // ~ no-op
    }
    @Override
    public void notifyEndOfWindow(EndOfWindow eow, Subscriber src,
                                  Collection<Subscriber> excludes)
    {
      // ~ no-op
    }
  }

  class NotifyingInstance implements EndOfWindowBroadcast {
    private final List<Subscriber> subscribers = new CopyOnWriteArrayList<>();

    @Override
    public void subscribe(Subscriber s) {
      subscribers.add(Objects.requireNonNull(s));
    }

    @Override
    public void notifyEndOfWindow(EndOfWindow eow, Subscriber src,
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

  void subscribe(Subscriber s);

  void notifyEndOfWindow(EndOfWindow eow, Subscriber src,
                         Collection<Subscriber> excludes);
}
