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
package org.apache.beam.sdk.testing;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import java.io.Serializable;
import org.hamcrest.Matcher;

/**
 * A {@link Matcher} that is also {@link Serializable}.
 *
 * <p>Such matchers can be used with {@link PAssert}, which builds pipelines such that these
 * matchers may be serialized and executed remotely.
 *
 * <p>To create a {@code SerializableMatcher}, extend {@link org.hamcrest.BaseMatcher} and also
 * implement this interface.
 *
 * @param <T> The type of value matched.
 */
@JsonSerialize(using = MatcherSerializer.class)
@JsonDeserialize(using = MatcherDeserializer.class)
public interface SerializableMatcher<T> extends Matcher<T>, Serializable {}
