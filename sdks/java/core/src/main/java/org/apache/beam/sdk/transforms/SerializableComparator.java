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
package org.apache.beam.sdk.transforms;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Objects;

/**
 * A {@code Comparator} that is also {@code Serializable}.
 *
 * @param <T> type of values being compared
 */
public interface SerializableComparator<T> extends Comparator<T>, Serializable {
	/**
	 * Returns a {@code SerializableComparator} that compares two objects by applying a key extractor function on them
	 * and comparing the results using their natural ordering. This is analogous to {@code Comparator.comparing()}.
	 *
	 * @param <T> the type of objects to be compared
	 * @param <U> the type of the Comparable keys to be extracted from the objects
	 * @param keyExtractor the function used to extract the Comparable keys from the objects
	 * @return a {@code SerializableComparator} that compares the objects by comparing the keys extracted from them
	 * @throws NullPointerException if the argument is null
	 */
	public static <T, U extends Comparable<U>> SerializableComparator<T> comparing(SerializableFunction<T, U> keyExtractor) {
		Objects.requireNonNull(keyExtractor);
		return (c1, c2) -> keyExtractor.apply(c1).compareTo(keyExtractor.apply(c2));
	}
}
