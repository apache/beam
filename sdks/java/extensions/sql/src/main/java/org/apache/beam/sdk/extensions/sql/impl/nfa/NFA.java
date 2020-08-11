package org.apache.beam.sdk.extensions.sql.impl.nfa;

import org.apache.beam.sdk.extensions.sql.impl.cep.CEPOperation;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPPattern;
import org.apache.beam.sdk.extensions.sql.impl.cep.Quantifier;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class NFA implements Serializable {

  // State are determined directly by the PATTERN clause
  private class State implements Serializable {
    private final String patternVar;
    private final Quantifier quant;
    private final CEPOperation condition; // condition to evaluate when taking the "begin" action and "proceed" action
    private State nextState = null;
    private final boolean isStart;
    private final boolean isFinal;
    private final boolean isKleenePlusSecondary; // whether is the second state for a Kleene Plus pattern variable

    State(String patternVar, Quantifier quant, CEPOperation condition, boolean isStart, boolean isFinal, boolean isKleenePlusSecondary) {
      this.patternVar = patternVar;
      this.quant = quant;
      this.condition = condition;
      this.isStart = isStart;
      this.isFinal = isFinal;
      this.isKleenePlusSecondary = isKleenePlusSecondary;
    }

    public void addNextState(State theNextState) {

      // TODO: check self-assignment
      if(this == theNextState) {
        return;
      }

      this.nextState = theNextState;
    }

    public State getNextState() {
      return nextState;
    }

    // constructs states for the NFA and returns the start state
    public List<State> loadStates(List<CEPPattern> patterns) {
      boolean startState;
      ArrayList<State> states = new ArrayList<>();

      for(int i = 0; i < patterns.size(); ++i) {
        if(i == 0) {
          startState = true;
        } else {
          startState = false;
        }

        CEPPattern currentPattern = patterns.get(i);
        CEPOperation condition = currentPattern.getPatternCondition();
        Quantifier quantifier = currentPattern.getQuantifier();

        if(quantifier == Quantifier.PLUS) {
          // for Kleene plus, we need a pair of states

          State primaryState = new State(
              currentPattern.getPatternVar(),
              Quantifier.PLUS,
              condition,
              startState,
              false,
              false
              );

          State secondaryState = new State(
              currentPattern.getPatternVar(),
              Quantifier.PLUS,
              condition,
              startState,
              false,
              true
              );

          primaryState.addNextState(secondaryState);
          states.add(primaryState);
        } else {
          // for non-Kleene-Plus pattern var, construct a single state
          State newState = new State(
              currentPattern.getPatternVar(),
              quantifier,
              condition,
              startState,
              false,
              false
              );
          states.add(newState);
        }
      }
      // add final state
      State theFinalState = new State(
          "",
          Quantifier.NONE,
          null,
          false,
          true,
          false
      );
      states.add(theFinalState);
      return states;
    }
  }
}
