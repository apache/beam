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
package org.apache.beam.runners.core.construction;

import static java.util.Collections.emptySet;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.apache.beam.runners.core.construction.classloader.Classloaders;
import org.apache.beam.sdk.options.PipelineOptions;

/**
 * Utilities for working with classpath resources for pipelines.
 */
public class PipelineResources {

    /**
     * Attempts to detect all the resources the class loader has access to. This does not recurse
     * to class loader parents stopping it from pulling in resources from the system class loader.
     *
     * @param options     The pipelineOptions to use to filter the files found.
     * @param classLoader The URLClassLoader to use to detect resources to stage.
     * @return A list of absolute paths to the resources the class loader uses.
     * @throws IllegalArgumentException If either the class loader is not a URLClassLoader or one
     *         of the resources the class loader exposes is not a file resource.
     */
    public static List<String> detectClassPathResourcesToStage(final PipelineOptions options,
                                                               final ClassLoader classLoader) {
        try {
            final Set<File> parentFiles = classLoader == null
                    ? emptySet() : Classloaders.toFiles(classLoader.getParent()).collect(toSet());
            final Predicate<File> filter = toFilter(options);
            return Classloaders.toFiles(classLoader)
                    .filter(s -> !parentFiles.contains(s))
                    .filter(filter)
                    .map(f -> {
                        try {
                            return f.getCanonicalPath();
                        } catch (IOException e) {
                            return f.getAbsolutePath();
                        }
                    })
                    .collect(toList());
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static Predicate<File> toFilter(final PipelineOptions options) {
        if (options == null) {
            return f -> true;
        }
        final FilterableStagingFilesPipelineOptions opts = options
                .as(FilterableStagingFilesPipelineOptions.class);
        final String include = opts.getClassLoaderIncludeFilter();
        final String exclude = opts.getClassLoaderExcludeFilter();
        if (include == null && exclude == null) {
            return f -> true;
        }
        final Predicate<String> includeFilter = include == null
                ? v -> true : new PatternFilter(include);
        final Predicate<String> excludeFilter = exclude == null
                ? v -> false : new PatternFilter(exclude);
        final Predicate<String> predicate = includeFilter.and(excludeFilter.negate());
        return f -> predicate.test(f.getName());
    }

    private static class PatternFilter implements Predicate<String> {
        private final Pattern pattern;

        private PatternFilter(final String include) {
            this.pattern = Pattern.compile(include);
        }

        @Override
        public boolean test(final String value) {
            return pattern.matcher(value).matches();
        }
    }
}
