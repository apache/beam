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

import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import java.io.IOException;
import java.util.Set;

/**
 * A classloader that intercepts loading of specifically named classes. This classloader copies
 * the original classes definition and is useful for testing code which needs to validate usage
 * with multiple classloaders..
 */
public class InterceptingUrlClassLoader extends ClassLoader {
    private final Set<String> ownedClasses;

    public InterceptingUrlClassLoader(final ClassLoader parent, final String... ownedClasses) {
        super(parent);
        this.ownedClasses = Sets.newHashSet(ownedClasses);
    }

    @Override
    public Class<?> loadClass(final String name) throws ClassNotFoundException {
        final Class<?> alreadyLoaded = super.findLoadedClass(name);
        if (alreadyLoaded != null) {
            return alreadyLoaded;
        }

        if (name != null && ownedClasses.contains(name)) {
            try {
                final String classAsResource = name.replace('.', '/') + ".class";
                final byte[] classBytes =
                        ByteStreams.toByteArray(getParent().getResourceAsStream(classAsResource));
                return defineClass(name, classBytes, 0, classBytes.length);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
        return getParent().loadClass(name);
    }
}
