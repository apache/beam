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

package org.apache.beam.sdk.util;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Specialization of {{@link ApiSurface}} that exposes the API surface for the Core SDK.
 */
public class CoreSdkApiSurface extends ApiSurface {
    private static final Logger LOG = LoggerFactory.getLogger(CoreSdkApiSurface.class);
    public CoreSdkApiSurface(Set<Class<?>> rootClasses, Set<Pattern> patternsToPrune) {
        super(rootClasses, patternsToPrune);
    }
    /**
     * All classes transitively reachable via only public method signatures of the SDK.
     *
     * <p>Note that our idea of "public" does not include various internal-only APIs.
     */
    @Override
    public ApiSurface buildApiSurface() throws IOException {
        ApiSurface surface =  ofRootClassesAndPatternsToPrune(Collections.<Class<?>>emptySet(),
                Collections.<Pattern>emptySet()).ofPackage("org.apache.beam",
                getClass().getClassLoader())
                .pruningPattern("org[.]apache[.]beam[.].*Test")
                // Exposes Guava, but not intended for users
                .pruningClassName("org.apache.beam.sdk.util.common.ReflectHelpers")
                .pruningPrefix("java");
        return new CoreSdkApiSurface(surface.getRootClasses(), surface.getPatternsToPrune());
    }

    @Override
    protected ApiSurface ofRootClassesAndPatternsToPrune(Set<Class<?>> rootClasses,
                                                         Set<Pattern> patternsToPrune) {
        return new CoreSdkApiSurface(Collections.<Class<?>>emptySet(),
                Collections.<Pattern>emptySet());
    }
}
