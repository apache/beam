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
package org.apache.beam.sdk.testing.junit5;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import org.apache.beam.sdk.testing.junit5.internal.BeamJUnit5Extension;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Marks a class or method as using beam test pipeline.
 */
@Inherited
@Retention(RUNTIME)
@Target({TYPE, METHOD})
@ExtendWith(BeamJUnit5Extension.class)
public @interface WithApacheBeam {
    /**
     * @return the pipeline options for the test.
     */
    String[] options() default {};

    /**
     * @return true if the pipeline is automatically ran.
     */
    boolean enableAutoRunIfMissing() default false;

    /**
     * @return true if all the PAssert should be visited to validate the result.
     */
    boolean enableAbandonedNodeEnforcement() default false;

    /**
     * @return by default a runner enforcement is done on the test,
     * i.e. if no runner is available a test requiring a runner with
     * the tag NeedsRunner will fail. Setting it to false will just
     * disable the test.
     */
    boolean skipMissingRunnerTests() default false;
}
