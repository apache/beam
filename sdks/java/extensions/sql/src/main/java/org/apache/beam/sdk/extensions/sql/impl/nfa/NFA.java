package org.apache.beam.sdk.extensions.sql.impl.nfa;

import org.apache.beam.sdk.extensions.sql.impl.cep.CEPCall;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPFieldRef;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPKind;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPLiteral;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPOperation;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPPattern;
import org.apache.beam.sdk.extensions.sql.impl.cep.Quantifier;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

// TODO: add support for more quantifiers: `?`, `{19, }` ... for now, support `+` and singletons
// TODO: sort conditions based on "the last identifier" during compilation
// TODO: add optimization for the NFA
// for now, assume the conditions are sorted.
// This implementation is single-threaded, the multi-threaded (parallel) version is left as a future work
public class NFA implements Serializable {

  private final State startState;
  private ArrayList<StateLocator> currentRuns;
  private final Schema outSchema;

  private NFA(List<CEPPattern> patterns, Schema outSchema) {
    this.startState = loadStates(patterns);
    this.currentRuns = new ArrayList<>();
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

  public boolean processNewEvent(Row inputRow) {
    // decide next possible states
    for(StateLocator i : currentRuns) {
      State nextTakeState = i.take(inputRow);
    }
  }

  // extracts the field reference from one side of the condition
  private static CEPFieldRef getFieldRefFromSideCondition(CEPCall sidedCondition) {
    List<CEPOperation> operands = sidedCondition.getOperands();
    for(CEPOperation operation : operands) {
      if(operation.getClass() == CEPFieldRef.class) {
        return (CEPFieldRef) operation;
      } else if(operation.getClass() == CEPCall.class) {
        CEPFieldRef tryRef = getFieldRefFromSideCondition((CEPCall) operation);
        if(tryRef != null) {
          return tryRef;
        }
      }
    }
    return null;
  }

  private class Event implements Serializable {
    private HashMap<EventPointer, Event> prevEvents = new HashMap<>(); // a mapping from the runIndex to the previous events
    private CEPLiteral content;

    // store an input row as a literal
    private CEPLiteral rowToCEPLiteral(Row inputRow, CEPFieldRef fieldRef) {
      int fieldIndex = fieldRef.getIndex();
      Schema.Field field = outSchema.getField(fieldIndex);
      Schema.FieldType type = field.getType();
      switch(type.getTypeName()) {
        case BYTE:
          return CEPLiteral.of(inputRow.getByte(fieldIndex));
        case INT16:
          return CEPLiteral.of(inputRow.getInt16(fieldIndex));
        case INT32:
          return CEPLiteral.of(inputRow.getInt32(fieldIndex));
        case INT64:
          return CEPLiteral.of(inputRow.getInt64(fieldIndex));
        case DECIMAL:
          return CEPLiteral.of(inputRow.getDecimal(fieldIndex));
        case FLOAT:
          return CEPLiteral.of(inputRow.getFloat(fieldIndex));
        case DOUBLE:
          return CEPLiteral.of(inputRow.getDouble(fieldIndex));
        case DATETIME:
          return CEPLiteral.of(inputRow.getDateTime(fieldIndex));
        case BOOLEAN:
          return CEPLiteral.of(inputRow.getBoolean(fieldIndex));
        case STRING:
          return CEPLiteral.of(inputRow.getString(fieldIndex));
        default:
          throw new UnsupportedOperationException("The type is not supported: " + type.getTypeName().toString());
      }
    }

    Event(Row inputRow, CEPFieldRef fieldRef) {
      this.content = rowToCEPLiteral(inputRow, fieldRef);
    }

    public void addPrevEvent(EventPointer ptr, @Nullable Event prevEvent) {
      prevEvents.put(ptr, prevEvent);
    }
  }

  // shared buffer versioned pointer: see the UMASS paper for description
  private class EventPointer implements Serializable {
    private final List<Integer> ptrValues;

    EventPointer(List<Integer> ptrValues) {
      this.ptrValues = ptrValues;
    }

    public EventPointer copy() {
      ArrayList<Integer> newPtrValue = new ArrayList<>(ptrValues);
      return new EventPointer(newPtrValue);
    }

    public EventPointer getNewProceedPointer(int value) {
      ArrayList<Integer> newPtrValue = new ArrayList<>(ptrValues);
      newPtrValue.add(value);
      return new EventPointer(newPtrValue);
    }

    // for taking the take edge with splitting, set last pointer digit to the desired value
    public EventPointer getNewTakePointer(int value) {
      ArrayList<Integer> newPtrValue = new ArrayList<>(ptrValues);
      newPtrValue.set(newPtrValue.size() - 1, value);
      return new EventPointer(newPtrValue);
    }

    @Override
    public String toString() {
      ArrayList<String> ptrValuesCopy = new ArrayList<>();
      for(Integer i : ptrValues) {
        ptrValuesCopy.add(i.toString());
      }
      String output = String.join(".", ptrValuesCopy);
      return output;
    }
  }

  // StateLocator should be thought as immutable
  private class StateLocator implements Serializable {
    private EventPointer ptr;
    private State curState;
    private int takeCount = 0; // counts the number of events taken
    private Event curEvent = null;

    StateLocator(EventPointer ptr, State curState, int takeCount, Event curEvent) {
      this.ptr = ptr;
      this.curState = curState;
      this.takeCount = takeCount;
      this.curEvent = curEvent;
    }

    // represents the "proceed"/"begin" edge: transfer to a new state,
    // write the new event in the new state's buffer.
    // returns a "new" state locator
    // returns null if not a match
    public StateLocator proceed(Event inputEvent) {
      if(curState.hasProceed()) {
        // try to proceed
        CEPCall condition = (CEPCall) curState.getProceedCondition();
        if(evalCondition(inputEvent, condition)) {
          // special case: for an event that starts a match,
          // assign a new index as the pointer value
          if(curState == startState && curEvent == null) {
            int ptrValue = curState.assignIndex();
            ArrayList<Integer> ptrArray = new ArrayList<>();
            ptrArray.add(ptrValue);
            EventPointer eventPointer = new EventPointer(ptrArray);
            inputEvent.addPrevEvent(eventPointer, null);
            return new StateLocator(eventPointer, curState.getNextState(), 0, inputEvent);
          }
          // for the other cases, add a zero in the event pointer
          EventPointer newPtr = ptr.getNewProceedPointer(0);
          inputEvent.addPrevEvent(newPtr, curEvent);
          return new StateLocator(newPtr, curState.getNextState(), 0, inputEvent);
        } else {
          return null;
        }
      } else {
        return null;
      }
    }

    // represents the "take" edge: the state stays at current state,
    // write the new event in the current state's buffer.
    // returns null if not a match
    public StateLocator take(Event inputEvent, boolean split) {
      if(curState.hasTake()) {
        if(split) {
          // get another unique pointer value
          int ptrValue = curState.assignIndex();
          EventPointer newPtr = ptr.getNewTakePointer(ptrValue);
          inputEvent.addPrevEvent(newPtr, curEvent);
          return new StateLocator(newPtr, curState, takeCount + 1, inputEvent);
        } else {
          EventPointer newPtr = ptr.copy();
          inputEvent.addPrevEvent(newPtr, curEvent);
          return new StateLocator(newPtr, curState, takeCount + 1, inputEvent);
        }
      } else {
        return null;
      }
    }

    // evaluates the condition
    // returns true if the new event satisfies the condition at a givens state
    private boolean evalCondition(Event inputEvent, CEPOperation operation) {
      if(operation == null) {
        return true;
      }
      CEPCall condition = (CEPCall) operation;
      CEPKind comparator = condition.getOperator().getCepKind();
      CEPOperation leftSide = condition.getOperands().get(0); // left side must contain field reference
      CEPOperation rightSide = condition.getOperands().get(1);
      CEPLiteral leftSideValue = evalLeftSideCondition(leftSide, inputEvent);
      CEPLiteral rightSideValue = evalRightSideCondition(rightSide);
      switch(comparator) {
        case EQUALS:
          return leftSideValue.compareTo(rightSideValue) == 0;
        case NOT_EQUALS:
          return leftSideValue.compareTo(rightSideValue) != 0;
        case GREATER_THAN:
          return leftSideValue.compareTo(rightSideValue) > 0;
        case GREATER_THAN_OR_EQUAL:
          return leftSideValue.compareTo(rightSideValue) >= 0;
        case LESS_THAN:
          return leftSideValue.compareTo(rightSideValue) < 0;
        case LESS_THAN_OR_EQUAL:
          return leftSideValue.compareTo(rightSideValue) <= 0;
        default:
          throw new IllegalStateException("the comparator is not supported: " + comparator.toString());
      }
    }

    // evaluates the condition value (left) given an input event
    private CEPLiteral evalLeftSideCondition(CEPOperation inputOperation, Event inputEvent) {
    }

    // evaluates the condition value (right) given an input event
    private CEPLiteral evalRightSideCondition(CEPOperation inputOperation) {
    }

  }


  // State are determined directly by the PATTERN clause
  private static class State implements Serializable {
    private int index = -1; // number ith state in the NFA
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

    public void setIndex(int indexToAssign) {
      this.index = indexToAssign;
    }

    // assigns a new index
    public int assignIndex() {
      return ++index;
    }

    // checks if a state has a take edge
    public boolean hasTake() {
      return isKleenePlusSecondary;
    }

    // checks if a state has a proceed/begin edge
    public boolean hasProceed() {
      // for singleton state and first state p[1] of a paired state
      // return true
      return !(isFinal || isKleenePlusSecondary);
    }

    // get the condition for the `take` condition
    // returns null if the take edge takes no condition
    // throws exception if there is no take edge for the state
    public CEPOperation getTakeCondition() {
      if(!hasTake()) {
        throw new IllegalStateException("The state does not have a take edge.");
      }
      return null;
    }

    // get the condition for the `proceed` or the `begin` edge
    // returns null if the proceed (begin) edge is always evaluated to true
    // throws exception if there is no proceed (begin) edge for the state
    public CEPOperation getProceedCondition() {
      if(!hasProceed()) {
        throw new IllegalStateException("The state does not have a proceed edge.");
      }
      // for singleton state or the first state of a paired state
      // return the current condition
      if(!isKleenePlusSecondary) {
        return condition;
      } else {
        return null;
      }
    }

    public Quantifier getQuantifier() {
      return quant;
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
  }

  private static State setNextStatesAndAssignIndices(List<State> states) {
    for(int i = 0; i < (states.size() - 1); ++i) {
      State currentState = states.get(i);
      currentState.setIndex(i);
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
    State beginState = setNextStatesAndAssignIndices(states);
    return beginState;
  }
}
