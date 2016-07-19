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
package com.google.common.base;


import java.util.NoSuchElementException;
import java.util.Set;
import javax.annotation.Nullable;

/***
 * Same as {@link Optional}, but throws {@link NoSuchElementException} for missing element.
 */
public class AbsentWithNoSuchElement<T> extends Optional<T> {
    private static final AbsentWithNoSuchElement INSTANCE = new
            AbsentWithNoSuchElement();
    private static final long serialVersionUID = 0L;

    public static <T> Optional<T> withType() {
        return INSTANCE;
    }

    @Override
    public boolean isPresent() {
        return Optional.<T>absent().isPresent();
    }

    @Override
    public T get() {
        throw new NoSuchElementException();
    }

    @Override
    public T or(T t) {
        return Optional.<T>absent().or(t);
    }

    @Override
    public Optional<T> or(Optional<? extends T> optional) {
        return Optional.<T>absent().or(optional);
    }

    @Override
    public T or(Supplier<? extends T> supplier) {
        return Optional.<T>absent().or(supplier);
    }

    @Nullable
    @Override
    public T orNull() {
        return Optional.<T>absent().orNull();
    }

    @Override
    public Set<T> asSet() {
        return Optional.<T>absent().asSet();
    }

    @Override
    public <V> Optional<V> transform(Function<? super T, V> function) {
        return Optional.<T>absent().transform(function);
    }

    @Override
    public boolean equals(Object o) {
        return o == this;
    }

    @Override
    public int hashCode() {
        return Optional.<T>absent().hashCode();
    }

    @Override
    public String toString() {
        return Optional.<T>absent().toString();
    }
}
