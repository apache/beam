/*
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

 package com.playground.extract_symbols;

import java.util.*;
import java.util.stream.Collectors;

public class ClassInfo {
    final Set<String> publicMethods = new HashSet<>();
    final Set<String> publicFields = new HashSet<>();

    Map<String, List<String>> toMap() {
        Map<String, List<String>> map = new HashMap<>();
        if (!publicMethods.isEmpty()) {
            map.put("methods", publicMethods.stream().sorted().collect(Collectors.toList()));
        }
        if (!publicFields.isEmpty()) {
            map.put("properties", publicFields.stream().sorted().collect(Collectors.toList()));
        }
        return map;
    }
}
