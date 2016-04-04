
package cz.seznam.euphoria.core.client.dataset;

import cz.seznam.euphoria.core.client.operator.State;

import java.util.HashSet;
import java.util.Set;

/**
 * Abstract window capable of registering states.
 */
public abstract class AbstractWindow<KEY, W extends AbstractWindow<KEY, W>>
    implements Window<KEY, W> {

  final Set<State<?, ?>> states = new HashSet<>();

  @Override
  public void addState(State<?, ?> state) {
    states.add(state);
  }

  @Override
  public Set<State<?, ?>> getStates() {
    return states;
  }

  void clearStates() {
    states.clear();
  }

}
