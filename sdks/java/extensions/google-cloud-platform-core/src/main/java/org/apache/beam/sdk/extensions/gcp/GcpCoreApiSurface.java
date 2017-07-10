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
package org.apache.beam.sdk.extensions.gcp;



import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.beam.sdk.util.ApiSurface;
/**
 * Specialization of {{@link ApiSurface}} that exposes the API surface for the GCP Core SDK.
 */
public class GcpCoreApiSurface extends ApiSurface {
    public GcpCoreApiSurface(Set<Class<?>> rootClasses, Set<Pattern> patternsToPrune) {
        super(rootClasses, patternsToPrune);
    }
    @Override
    public ApiSurface buildApiSurface() throws IOException {
        final Package thisPackage = getClass().getPackage();
        final ClassLoader thisClassLoader = getClass().getClassLoader();
        final ApiSurface apiSurface = new GcpCoreApiSurface(Collections.<Class<?>>emptySet(),
                Collections.<Pattern>emptySet())
                        .ofPackage(thisPackage, thisClassLoader)
                        .pruningClass(GcpCoreApiSurface.class)
                        .pruningPattern("org[.]apache[.]beam[.].*Test.*")
                        .pruningPattern("org[.]apache[.]beam[.].*IT")
                        .pruningPattern("java[.]lang.*")
                        .pruningPattern("java[.]util.*");
        return apiSurface;
    }

    @Override
    protected ApiSurface ofRootClassesAndPatternsToPrune(Set<Class<?>> rootClasses,
                                                         Set<Pattern> patternsToPrune) {
        return new GcpCoreApiSurface(rootClasses, patternsToPrune);
    }
}
