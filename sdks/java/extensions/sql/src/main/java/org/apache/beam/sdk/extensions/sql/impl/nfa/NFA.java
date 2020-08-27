/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.extensions.sql.impl.nfa;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPCall;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPFieldRef;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPKind;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPLiteral;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPOperation;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPOperator;
import org.apache.beam.sdk.extensions.sql.impl.cep.CEPPattern;
import org.apache.beam.sdk.extensions.sql.impl.cep.Quantifier;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.values.Row;

// TODO: add support for more quantifiers: `?`, `{19, }` ... for now, support `+` and singletons
// TODO: sort conditions based on "the last identifier" during compilation
// TODO: add optimization for the NFA
// for now, assume the conditions are sorted.

/**
 * {@code NFA} is an implementation of non-deterministic finite automata. It is used for the
 * non-deterministic pattern match in the MATCH_RECOGNIZE component.
 *
 * <p>The implementation is strongly based on the UMASS paper on NFA with shared buffer:
 *
 * @see <a
 *     href="https://dl.acm.org/doi/10.1145/1376616.1376634">https://dl.acm.org/doi/10.1145/1376616.1376634</a>
 */
public class NFA implements Serializable {

  private final State startState;
  private ArrayList<StateLocator> currentRuns;
  private final Schema upstreamSchema;

  private NFA(List<CEPPattern> patterns, Schema upstreamSchema) {
    this.startState = loadStates(patterns);
    this.currentRuns = new ArrayList<>();
    this.upstreamSchema = upstreamSchema;
  }

  public static NFA compile(List<CEPPattern> patterns, Schema outSchema) {
    return new NFA(patterns, outSchema);
  }

  // process a new row,
  // return a mapping of pattern variable to row to output in the DoFn if there is a match
  // return null if none of locators reached the final state
  public Map<String, ArrayList<Row>> processNewRow(Row inputRow) {
    // wrap the input row as an event
    Event inputEvent = new Event(inputRow, upstreamSchema);
    ArrayList<StateLocator> nextStateLocators = new ArrayList<>();
    // add a start state locator to the array
    EventPointer nullPtr = new EventPointer(new ArrayList<>(), "");
    currentRuns.add(new StateLocator(nullPtr, startState, startState, 0, null));
    // scan for kleene plus locator, if exits, add the next next state
    ArrayList<StateLocator> kleenePlusLocators = new ArrayList<>();
    for (StateLocator locator : currentRuns) {
      if (locator.isKleenePlusSecondary()) {
        if (locator.getCurState().getNextState().isFinal) {
          return processOutput(locator);
        }
        StateLocator proceedLocator = locator.proceedIgnore();
        kleenePlusLocators.add(proceedLocator);
      }
    }
    currentRuns.addAll(kleenePlusLocators);

    // decide next possible states
    for (StateLocator currentLocator : currentRuns) {
      boolean split = false;
      StateLocator proceedLocator = currentLocator.proceed(inputEvent);
      if (proceedLocator != null) {
        // TODO: add support for after match strategy
        // if the locator is at the final state
        // clear current runs
        // then return the output
        if (proceedLocator.atFinal()) {
          // reset after a match
          currentRuns.clear();
          State iterState = startState;
          while (!iterState.isFinal) {
            iterState.reset();
            iterState = iterState.getNextState();
          }
          currentRuns.add(new StateLocator(nullPtr, startState, startState, 0, null));

          return processOutput(proceedLocator);
        } else {
          // add the new locator to the array
          nextStateLocators.add(proceedLocator);
        }
        split = true;
      }
      StateLocator takeLocator = currentLocator.take(inputEvent, split);
      if (takeLocator != null) {
        nextStateLocators.add(takeLocator);
      }
    }
    this.currentRuns = nextStateLocators;
    return null;
  }

  // returns a row with the output schema once a locator reaches the final state
  private Map<String, ArrayList<Row>> processOutput(StateLocator locator) {
    HashMap<String, ArrayList<Row>> rows = new HashMap<>();
    EventPointer iterPointer = locator.getPointer().copy();
    Event curEvent = locator.getCurrentEvent();

    while (curEvent != null) {
      String patternVar = iterPointer.getPatternVar();
      if (rows.containsKey(patternVar)) {
        ArrayList<Row> patternArray = rows.get(patternVar);
        patternArray.add(curEvent.getRow());
      } else {
        ArrayList<Row> newPatternArray = new ArrayList<>();
        newPatternArray.add(curEvent.getRow());
        rows.put(patternVar, newPatternArray);
      }
      curEvent = curEvent.findEvent(iterPointer);
      if (curEvent != null) {
        iterPointer = curEvent.getPrevPointer(iterPointer);
      }
    }

    // restore the order
    for (ArrayList<Row> i : rows.values()) {
      Collections.reverse(i);
    }

    return rows;
  }

  private static class Event implements Serializable {
    private HashMap<EventPointer, Event> prevEvents =
        new HashMap<>(); // a mapping from the runIndex to the previous events
    private Row row;
    private Schema upstreamSchema;

    Event(Row inputRow, Schema upstreamSchema) {
      this.row = inputRow;
      this.upstreamSchema = upstreamSchema;
    }

    // store an input row as a literal
    public CEPLiteral toCEPLiteral(CEPFieldRef fieldRef) {
      int fieldIndex = fieldRef.getIndex();
      Schema.Field field = upstreamSchema.getField(fieldIndex);
      Schema.FieldType type = field.getType();
      switch (type.getTypeName()) {
        case BYTE:
          return CEPLiteral.of(row.getByte(fieldIndex));
        case INT16:
          return CEPLiteral.of(row.getInt16(fieldIndex));
        case INT32:
          return CEPLiteral.of(row.getInt32(fieldIndex));
        case INT64:
          return CEPLiteral.of(row.getInt64(fieldIndex));
        case DECIMAL:
          return CEPLiteral.of(row.getDecimal(fieldIndex));
        case FLOAT:
          return CEPLiteral.of(row.getFloat(fieldIndex));
        case DOUBLE:
          return CEPLiteral.of(row.getDouble(fieldIndex));
        case DATETIME:
          return CEPLiteral.of(row.getDateTime(fieldIndex));
        case BOOLEAN:
          return CEPLiteral.of(row.getBoolean(fieldIndex));
        case STRING:
          return CEPLiteral.of(row.getString(fieldIndex));
        default:
          throw new UnsupportedOperationException(
              "The type is not supported: " + type.getTypeName().toString());
      }
    }

    private Event findEvent(EventPointer eventPointer) {
      return prevEvents.getOrDefault(eventPointer, null);
    }

    private EventPointer findEventPointer(EventPointer pointer) {
      for (EventPointer i : prevEvents.keySet()) {
        if (i.equals(pointer)) {
          return i.copy();
        }
      }
      return null;
    }

    public EventPointer getPrevPointer(EventPointer curPointer) {
      if (curPointer.isNull()) {
        return null;
      }
      if (curPointer.isProceedPointer()) {
        while (findEvent(curPointer) == null && curPointer.canTrim()) {
          curPointer.trim();
        }
        return findEventPointer(curPointer);
      } else {
        while (findEvent(curPointer) == null) {
          if (curPointer.canDecrement()) {
            curPointer.decrement();
          } else {
            return null;
          }
        }
        return curPointer;
      }
    }

    public Row getRow() {
      return row;
    }

    public void addPrevEvent(EventPointer ptr, @Nullable Event prevEvent) {
      prevEvents.put(ptr, prevEvent);
    }
  }

  // shared buffer versioned pointer: see the UMASS paper for description
  private static class EventPointer implements Serializable {
    private List<Integer> ptrValues;
    // labels the event that was pointed to
    private final String patternVar;

    EventPointer(List<Integer> ptrValues, String patternVar) {
      this.ptrValues = ptrValues;
      this.patternVar = patternVar;
    }

    public boolean canTrim() {
      return ptrValues.size() > 1;
    }

    // for following a proceed edge
    // trim the last digit
    public void trim() {
      if (isProceedPointer()) {
        ptrValues = ptrValues.subList(0, ptrValues.size() - 1);
      } else {
        throw new IllegalStateException("the null event pointer cannot be trimmed.");
      }
    }

    public boolean canDecrement() {
      if (isNull()) {
        return false;
      }
      int lastValue = ptrValues.get(ptrValues.size() - 1);
      return lastValue > 0;
    }

    // for following the take edge
    // decrement the last digit
    public void decrement() {
      if (!canDecrement()) {
        throw new IllegalStateException("the event pointer cannot be decremented.");
      }
      if (!isProceedPointer()) {
        ArrayList<Integer> newPtrValue =
            new ArrayList<>(ptrValues.subList(0, ptrValues.size() - 1));
        int lastValue = ptrValues.get(ptrValues.size() - 1) - 1;
        newPtrValue.add(lastValue);
        ptrValues = newPtrValue;
      } else {
        throw new IllegalStateException("the event pointer cannot be decremented.");
      }
    }

    @Override
    public boolean equals(Object other) {
      if (!(other instanceof EventPointer)) {
        return false;
      }
      EventPointer otherPointer = (EventPointer) other;
      return ptrValues.equals(otherPointer.ptrValues);
    }

    public boolean isProceedPointer() {
      // if the last digit of the pointer is 0,
      // then it is a proceed pointer
      if (ptrValues.isEmpty()) {
        return false;
      }
      int lastPtrValue = ptrValues.get(ptrValues.size() - 1);
      if (lastPtrValue == 0) {
        return true;
      } else {
        return false;
      }
    }

    @Override
    public int hashCode() {
      return ptrValues.hashCode();
    }

    public boolean isNull() {
      return ptrValues.isEmpty() && patternVar.equals("");
    }

    public String getPatternVar() {
      return patternVar;
    }

    public EventPointer copy() {
      ArrayList<Integer> newPtrValue = new ArrayList<>(ptrValues);
      return new EventPointer(newPtrValue, patternVar);
    }

    public EventPointer getNewProceedPointer(int value, String patternVar) {
      ArrayList<Integer> newPtrValue = new ArrayList<>(ptrValues);
      newPtrValue.add(value);
      return new EventPointer(newPtrValue, patternVar);
    }

    // for taking the take edge with splitting, set last pointer digit to the desired value
    public EventPointer getNewTakePointer(int value) {
      ArrayList<Integer> newPtrValue = new ArrayList<>(ptrValues);
      newPtrValue.set(newPtrValue.size() - 1, value);
      return new EventPointer(newPtrValue, patternVar);
    }

    @Override
    public String toString() {
      ArrayList<String> ptrValuesCopy = new ArrayList<>();
      for (Integer i : ptrValues) {
        ptrValuesCopy.add(i.toString());
      }
      return String.join(".", ptrValuesCopy);
    }
  }

  // StateLocator should be thought as immutable
  private static class StateLocator implements Serializable {
    private EventPointer ptr;
    private State startState;
    private State curState;
    private int takeCount = 0; // counts the number of events taken
    private Event curEvent = null;

    StateLocator(
        EventPointer ptr, State startState, State curState, int takeCount, Event curEvent) {
      this.ptr = ptr;
      this.startState = startState;
      this.curState = curState;
      this.takeCount = takeCount;
      this.curEvent = curEvent;
    }

    public State getCurState() {
      return curState;
    }

    public EventPointer getPointer() {
      return ptr;
    }

    public Event getCurrentEvent() {
      return curEvent;
    }

    // check if the current state is the final state
    public boolean atFinal() {
      return curState.isFinal;
    }

    public boolean isKleenePlusSecondary() {
      return curState.isKleenePlusSecondary();
    }

    // a proceed action for secondary kleenePlus state
    public StateLocator proceedIgnore() {
      if (isKleenePlusSecondary()) {
        EventPointer newPtr = ptr.getNewProceedPointer(0, curState.getPatternVar());
        return new StateLocator(newPtr, startState, curState.getNextState(), 0, curEvent);
      } else {
        return null;
      }
    }

    // represents the "proceed"/"begin" edge: transfer to a new state,
    // writes the new event in the new state's buffer.
    // returns a "new" state locator
    // returns null if not a match
    public StateLocator proceed(Event inputEvent) {
      if (curState.hasProceed()) {
        // try to proceed
        CEPCall condition = (CEPCall) curState.getProceedCondition();
        String patternVar = curState.getPatternVar();
        if (evalCondition(inputEvent, condition)) {
          // special case: for an event that starts a match,
          // assign a new index as the pointer value
          if (curState == startState && curEvent == null) {
            int ptrValue = curState.assignIndex();
            ArrayList<Integer> ptrArray = new ArrayList<>();
            ptrArray.add(ptrValue);
            EventPointer eventPointer = new EventPointer(ptrArray, patternVar);
            inputEvent.addPrevEvent(eventPointer, null);
            return new StateLocator(
                eventPointer, startState, curState.getNextState(), 0, inputEvent);
          }
          // for the other cases, add a zero in the event pointer
          EventPointer newPtr = ptr.getNewProceedPointer(0, patternVar);
          inputEvent.addPrevEvent(newPtr, curEvent);
          return new StateLocator(newPtr, startState, curState.getNextState(), 0, inputEvent);
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
      if (curState.hasTake()) {
        if (evalCondition(inputEvent, curState.getTakeCondition())) {
          if (split) {
            // get another unique pointer value
            int ptrValue = curState.assignIndex();
            EventPointer newPtr = ptr.getNewTakePointer(ptrValue);
            inputEvent.addPrevEvent(newPtr, curEvent);
            return new StateLocator(newPtr, startState, curState, takeCount + 1, inputEvent);
          } else {
            EventPointer newPtr = ptr.copy();
            inputEvent.addPrevEvent(newPtr, curEvent);
            return new StateLocator(newPtr, startState, curState, takeCount + 1, inputEvent);
          }
        } else {
          return null;
        }
      } else {
        return null;
      }
    }

    // evaluates the condition
    // returns true if the new event satisfies the condition at a givens state
    private boolean evalCondition(Event inputEvent, CEPOperation operation) {
      if (operation == null) {
        return true;
      }
      CEPCall condition = (CEPCall) operation;
      CEPKind comparator = condition.getOperator().getCepKind();
      CEPOperation leftSide =
          condition.getOperands().get(0); // left side must contain field reference
      CEPOperation rightSide = condition.getOperands().get(1);
      CEPLiteral leftSideValue = evalLeftSideCondition(leftSide, inputEvent);
      CEPLiteral rightSideValue = evalRightSideCondition(rightSide, inputEvent);
      if (leftSideValue == null) {
        return false;
      }
      if (rightSideValue == null) {
        return true;
      }
      switch (comparator) {
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
          throw new IllegalStateException(
              "the comparator is not supported: " + comparator.toString());
      }
    }

    // evaluates the condition value (left) given an input event
    private CEPLiteral evalLeftSideCondition(CEPOperation inputOperation, Event inputEvent) {
      if (inputOperation instanceof CEPLiteral) {
        return (CEPLiteral) inputOperation;
      } else if (inputOperation.getClass() == CEPFieldRef.class) {
        return inputEvent.toCEPLiteral((CEPFieldRef) inputOperation);
      } else if (inputOperation.getClass() == CEPCall.class) {
        CEPCall call = (CEPCall) inputOperation;
        CEPOperator operator = call.getOperator();
        List<CEPOperation> operands = call.getOperands();
        switch (operator.getCepKind()) {
          case LAST:
            return last(
                operands.get(0), evalLeftSideCondition(operands.get(1), inputEvent), inputEvent);
          case PLUS:
            return plus(
                evalLeftSideCondition(operands.get(0), inputEvent),
                evalLeftSideCondition(operands.get(1), inputEvent));
          default:
            throw new UnsupportedOperationException(
                "the function is not supported for now: " + operator.getCepKind().toString());
        }
      } else {
        throw new IllegalStateException(
            "the left side CEP operation is not legal: " + inputOperation.getClass().toString());
      }
    }

    // evaluates the condition value (right) given an input event
    private CEPLiteral evalRightSideCondition(CEPOperation inputOperation, Event inputEvent) {
      if (inputOperation instanceof CEPLiteral) {
        return (CEPLiteral) inputOperation;
      } else if (inputOperation.getClass() == CEPCall.class) {
        CEPCall call = (CEPCall) inputOperation;
        CEPOperator operator = call.getOperator();
        List<CEPOperation> operands = call.getOperands();
        switch (operator.getCepKind()) {
          case PLUS:
            return plus(
                evalRightSideCondition(operands.get(0), inputEvent),
                evalRightSideCondition(operands.get(1), inputEvent));
          case PREV:
            return prev(operands.get(0), (CEPLiteral) operands.get(1), ptr, curEvent, inputEvent);
          default:
            throw new UnsupportedOperationException(
                "the function is not supported for now: " + operator.getCepKind().toString());
        }
      } else {
        throw new IllegalStateException(
            "the right side CEP operation is not legal: " + inputOperation.getClass().toString());
      }
    }

    /* below are function implementations */

    // represents the PREV operation
    private CEPLiteral prev(
        CEPOperation opr1,
        CEPLiteral opr2,
        EventPointer curPointer,
        Event curEvent,
        Event inputEvent) {
      if (opr1.getClass() != CEPFieldRef.class) {
        throw new IllegalStateException(
            "the first argument of the PREV operation should be a field reference. Provided: "
                + opr1.getClass().toString());
      }
      if (opr2.getTypeName() != Schema.TypeName.DECIMAL) {
        throw new IllegalStateException(
            "the second argument of the prev operation should be a decimal. Provided: "
                + opr2.getClass().toString());
      }
      // if the second argument is 0, return the current event
      if (opr2.getDecimal().intValue() == 0) {
        return inputEvent.toCEPLiteral((CEPFieldRef) opr1);
      }
      // for the starting event, return null
      if (curEvent == null) {
        return null;
      }
      CEPFieldRef fieldRef = (CEPFieldRef) opr1;
      String alpha = fieldRef.getAlpha(); // the patternVar
      int lastNumber = opr2.getDecimal().intValue();
      EventPointer iterPointer = curPointer.copy();

      while (curEvent != null && iterPointer.getPatternVar().equals(alpha) && lastNumber > 0) {
        iterPointer = curEvent.getPrevPointer(iterPointer);
        curEvent = curEvent.findEvent(iterPointer);
      }
      if (curEvent != null) {
        return curEvent.toCEPLiteral(fieldRef);
      } else {
        return null;
      }
    }

    // represents the LAST operation
    // TODO: add implementation. for now, return the current row
    private CEPLiteral last(CEPOperation opr1, CEPLiteral opr2, Event inputEvent) {
      if (opr1.getClass() != CEPFieldRef.class) {
        throw new IllegalStateException(
            "the first argument of the PREV operation should be a field reference. Provided: "
                + opr1.getClass().toString());
      }
      if (opr2.getTypeName() != Schema.TypeName.DECIMAL) {
        throw new IllegalStateException(
            "the second argument of the prev operation should be a decimal. Provided: "
                + opr2.getClass().toString());
      }
      return inputEvent.toCEPLiteral((CEPFieldRef) opr1);
    }

    // represents the PLUS operation
    private CEPLiteral plus(CEPLiteral opr1, CEPLiteral opr2) {
      Schema.TypeName type1 = opr1.getTypeName();
      Schema.TypeName type2 = opr2.getTypeName();
      if (type1.isNumericType() && type1 == type2) {
        switch (type1) {
          case BYTE:
            return CEPLiteral.of(opr1.getByte() + opr2.getByte());
          case INT16:
            return CEPLiteral.of(opr1.getInt16() + opr2.getInt16());
          case INT32:
            return CEPLiteral.of(opr1.getInt32() + opr2.getInt32());
          case INT64:
            return CEPLiteral.of(opr1.getInt64() + opr2.getInt64());
          case DECIMAL:
            return CEPLiteral.of(opr1.getDecimal().add(opr2.getDecimal()));
          case FLOAT:
            return CEPLiteral.of(opr1.getFloat() + opr2.getFloat());
          case DOUBLE:
            return CEPLiteral.of(opr1.getDouble() + opr2.getDouble());
          default:
            throw new UnsupportedOperationException("Type is not supported: " + type1.toString());
        }
      } else {
        throw new IllegalStateException(
            "Types do not match: " + type1.toString() + ", " + type2.toString());
      }
    }
  }

  // State are determined directly by the PATTERN clause
  private static class State implements Serializable {
    private int index = 0; // number ith state (pointer value) in the NFA
    private final String patternVar;
    private final Quantifier quant;
    private final CEPOperation
        condition; // condition to evaluate when taking the "begin" action and "proceed" action
    private State nextState = null;
    public final boolean isStart;
    public final boolean isFinal;
    private final boolean
        isKleenePlusSecondary; // whether is the second state for a Kleene Plus pattern variable

    State(
        String patternVar,
        Quantifier quant,
        CEPOperation condition,
        boolean isStart,
        boolean isFinal,
        boolean isKleenePlusSecondary) {
      this.patternVar = patternVar;
      this.quant = quant;
      this.condition = condition;
      this.isStart = isStart;
      this.isFinal = isFinal;
      this.isKleenePlusSecondary = isKleenePlusSecondary;
    }

    public String getPatternVar() {
      return patternVar;
    }

    // assigns a new index
    public int assignIndex() {
      return ++index;
    }

    // reset the index
    public void reset() {
      index = 0;
    }

    // checks if a state has a take edge
    public boolean hasTake() {
      return isKleenePlusSecondary;
    }

    // checks if a state has a proceed/begin edge
    public boolean hasProceed() {
      // except for the final state
      // return true
      return !isFinal;
    }

    public boolean isKleenePlusSecondary() {
      return isKleenePlusSecondary;
    }

    // get the condition for the `take` condition
    // returns null if the take edge takes no condition
    // throws exception if there is no take edge for the state
    public CEPOperation getTakeCondition() {
      if (!hasTake()) {
        throw new IllegalStateException("The state does not have a take edge.");
      }
      return condition;
    }

    // get the condition for the `proceed` or the `begin` edge
    // returns null if the proceed (begin) edge is always evaluated to true
    // throws exception if there is no proceed (begin) edge for the state
    public CEPOperation getProceedCondition() {
      if (!hasProceed()) {
        throw new IllegalStateException("The state does not have a proceed edge.");
      }
      // for singleton state or the first state of a paired state
      // return the current condition
      if (!isKleenePlusSecondary) {
        return condition;
      } else {
        return null;
      }
    }

    public Quantifier getQuantifier() {
      return quant;
    }

    public void setNextState(State theNextState) {

      if (this == theNextState) {
        return;
      }

      this.nextState = theNextState;
    }

    public State getNextState() {
      return nextState;
    }

    public boolean isKleenePlus() {
      return quant.toString().equals("+") || quant.toString().equals("+?");
    }
  }

  private static State setNextStatesAndAssignIndices(List<State> states) {
    for (int i = 0; i < (states.size() - 1); ++i) {
      State currentState = states.get(i);
      State nextState = states.get(i + 1);
      if (currentState.isKleenePlus()) {
        State secondaryState = currentState.getNextState();
        secondaryState.setNextState(nextState);
      } else {
        currentState.setNextState(nextState);
      }
    }
    return states.get(0);
  }

  // constructs states for the NFA and returns the start state
  private State loadStates(List<CEPPattern> patterns) {
    boolean startState;
    ArrayList<State> states = new ArrayList<>();

    for (int i = 0; i < patterns.size(); ++i) {
      if (i == 0) {
        startState = true;
      } else {
        startState = false;
      }

      CEPPattern currentPattern = patterns.get(i);
      CEPOperation condition = currentPattern.getPatternCondition();
      Quantifier quantifier = currentPattern.getQuantifier();

      if (quantifier.toString().equals("+")) {
        // for Kleene plus, we need a pair of states

        State primaryState =
            new State(
                currentPattern.getPatternVar(),
                Quantifier.PLUS,
                condition,
                startState,
                false,
                false);

        State secondaryState =
            new State(
                currentPattern.getPatternVar(),
                Quantifier.PLUS,
                condition,
                startState,
                false,
                true);

        primaryState.setNextState(secondaryState);
        states.add(primaryState);
      } else {
        // for non-Kleene-Plus pattern var, construct a single state
        State newState =
            new State(
                currentPattern.getPatternVar(), quantifier, condition, startState, false, false);
        states.add(newState);
      }
    }
    // add final state
    State theFinalState = new State("", Quantifier.NONE, null, false, true, false);
    states.add(theFinalState);
    State beginState = setNextStatesAndAssignIndices(states);
    return beginState;
  }
}
