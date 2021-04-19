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
package org.apache.beam.validate.runner.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class CategoryRetriever {
    public static List<String> getCategories(String className, String methodName) {
        List<String> categoryNames = new ArrayList<>();
        try {
            Class tempClass = Class.forName(className);
            for (Method method : tempClass.getDeclaredMethods()) {
                if(method.getName().equals(methodName)) {
                    Annotation[] annotations = method.getAnnotations();
                    for(Annotation annotation : annotations) {
                        if (annotation instanceof org.junit.experimental.categories.Category) {
                            Class<?>[] categories = null;
                            categories = ((org.junit.experimental.categories.Category) annotation).value();
                            Arrays.stream(categories).forEach(category -> categoryNames.add(category.getName()));
                        } else {
                            continue;
                        }
                        return categoryNames;
                    }
                } else {
                    continue;
                }
            }
        } catch (ClassNotFoundException e) {
            return categoryNames;
        }
        return new ArrayList<>();
    }
}
