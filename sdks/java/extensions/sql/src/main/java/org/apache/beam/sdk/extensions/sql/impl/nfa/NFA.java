package org.apache.beam.sdk.extensions.sql.impl.nfa;

import org.apache.beam.sdk.extensions.sql.impl.cep.CEPCall;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPLiteral;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPOperation;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPPattern;
import org.apache.beam.sdk.extensions.sql.impl.cep.Quantifier;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

// TODO: sort conditions based on "the last identifier" during compilation
// TODO: add optimization for the NFA
// for now, assume the conditions are sorted.
// This implementation is single-threaded, the multi-threaded (parallel) version is left as a future work
public class NFA implements Serializable {

  private final State startState;
  private ArrayList<State> currentRuns;
  private final Schema outSchema;

  private NFA(List<CEPPattern> patterns, Schema outSchema) {
    this.startState = loadStates(patterns);
    this.currentRuns = new ArrayList<>();
    this.currentRuns.add(startState);
    this.outSchema = outSchema;
  }

  public static NFA compile(List<CEPPattern> patterns, Schema outSchema) {
    return new NFA(patterns, outSchema);
  }

  // represents the PLUS operation
  private Number plus(CEPLiteral opr1, CEPLiteral opr2) {
    Schema.TypeName type1 = opr1.getTypeName();
    Schema.TypeName type2 = opr2.getTypeName();
    if(type1.isNumericType() && type1 == type2) {
      switch(type1) {
        case BYTE:
          return opr1.getByte() + opr2.getByte();
        case INT16:
          return opr1.getInt16() + opr2.getInt16();
        case INT32:
          return opr1.getInt32() + opr2.getInt32();
        case INT64:
          return opr1.getInt64() + opr2.getInt64();
        case DECIMAL:
          return opr1.getDecimal().add(opr2.getDecimal());
        case FLOAT:
          return opr1.getFloat() + opr2.getFloat();
        case DOUBLE:
          return opr1.getDouble() + opr2.getDouble();
        default:
          throw new UnsupportedOperationException("Type is not supported: " + type1.toString());
      }
    } else {
      throw new IllegalStateException("Types do not match: " + type1.toString() + ", " + type2.toString());
    }
  }

  public boolean process(Row inputRow) {
    // decide next possible states
    for(State i : currentRuns) {
      State nextTakeState = i.take(i, inputRow);
    }
  }

  private static class Event implements Serializable {
    private ArrayList<Integer> ptrs; // shared buffer ptrs

    private Event(Row inputRow) {

    }
  }

  private class StateLocator implements Serializable {
    private Event origin;

    StateLocator(Event origin) {
      this.origin = origin;
    }

  }


  // State are determined directly by the PATTERN clause
  private static class State implements Serializable {
    private final String patternVar;
    private final Quantifier quant;
    private final CEPOperation condition; // condition to evaluate when taking the "begin" action and "proceed" action
    private State nextState = null;
    private final boolean isStart;
    private final boolean isFinal;
    private final boolean isKleenePlusSecondary; // whether is the second state for a Kleene Plus pattern variable
    private ArrayList<Event> sharedBuffer; // shared buffer for simoutaneous runs

    State(String patternVar, Quantifier quant, CEPOperation condition, boolean isStart, boolean isFinal, boolean isKleenePlusSecondary) {
      this.patternVar = patternVar;
      this.quant = quant;
      this.condition = condition;
      this.isStart = isStart;
      this.isFinal = isFinal;
      this.isKleenePlusSecondary = isKleenePlusSecondary;
    }

    public CEPOperation getCondition() {
      return condition;
    }

    public void setNextState(State theNextState) {

      if(this == theNextState) {
        return;
      }

      this.nextState = theNextState;
    }

    public State getNextState() {
      return nextState;
    }

    public boolean isKleenePlus() {
      return this.quant == Quantifier.PLUS ||
          this.quant == Quantifier.PLUS_RELUCTANT;
    }



    // represents the "take" edge
    public State take(State curState, Row rowToEval) {
      CEPCall condition = (CEPCall) curState.getCondition();

      return null;
    }
  }

  private static State setNextStates(List<State> states) {
    for(int i = 0; i < (states.size() - 1); ++i) {
      State currentState = states.get(i);
      State nextState = states.get(i + 1);
      if(currentState.isKleenePlus()) {
        State secondaryState = currentState.getNextState();
        secondaryState.setNextState(nextState);
      } else {
        currentState.setNextState(nextState);
      }
    }
    return states.get(0);
  }

  // constructs states for the NFA and returns the start state
  private static State loadStates(List<CEPPattern> patterns) {
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

        primaryState.setNextState(secondaryState);
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
    State beginState = setNextStates(states);
    return beginState;
  }
}
