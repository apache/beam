package org.apache.beam.sdk.extensions.sql.impl.transform.agg;

import org.apache.beam.sdk.transforms.Combine;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

public class CountIf {

    private CountIf(){}

    /** Returns a {@link Combine.CombineFn} that counts the number of its inputs.
     * @return*/
    public static <T extends Number> CountIfFn<T> combineFn() {
        return new CountIf.CountIfFn<T>();
    }

    public static class CountIfFn<T extends Number> extends Combine.CombineFn<T, Integer, Integer> {

        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer addInput(Integer mutableAccumulator, T input) {
            try {
                mutableAccumulator += evaluateExpression(input);
            } catch (ScriptException e) {
                e.printStackTrace();
            }
            return mutableAccumulator;
        }

        @Override
        public Integer mergeAccumulators(Iterable<Integer> accumulators) {
            Integer count = 0;
            for (Integer accum : accumulators) {
                count += accum;
            }
            return count;
        }

        @Override
        public Integer extractOutput(Integer accumulator) {
            return accumulator;
        }

        // To convert an input string expression to mathematical representation and evaluates it to true or false
        public Integer evaluateExpression(T expression) throws ScriptException {
            Integer count = 0;
            ScriptEngineManager mgr = new ScriptEngineManager();
            ScriptEngine engine = mgr.getEngineByName("JavaScript");
            if (((boolean) engine.eval(String.valueOf(expression)))){
                count+=1;
            }
            return count;
        }
    }

}
