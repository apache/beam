/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cz.seznam.euphoria.core.client.functional;

/**
 * Reduce function reducing iterable of elements into single element (of
 * possibly different type).
 */
@FunctionalInterface
public interface ReduceFunction<IN, OUT> extends UnaryFunction<Iterable<IN>, OUT> {

}
