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
package org.apache.beam.agent;

import java.lang.instrument.Instrumentation;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/*
 * Opens all base modules in the JDK to jamm (used to accurately measure object sizes)
 */
public class OpenModuleAgent {
  public static void premain(String argument, Instrumentation instrumentation) {
    Set<Module> modulesToOpen = new HashSet<>();
    Optional<Module> jamm = ModuleLayer.boot().findModule("jamm");
    if (!jamm.isPresent()) {
      System.err.println("Jamm module expected, but not found");
      return;
    }

    ModuleLayer.boot()
        .modules()
        .forEach(
            module -> {
              if (!module.getDescriptor().isOpen()) {
                modulesToOpen.add(module);
              }
            });

    Set<Module> openModules = Set.of(jamm.get());
    for (Module module : modulesToOpen) {
      Map<String, Set<Module>> addOpens = new HashMap<>();
      for (String pkg : module.getPackages()) {
        addOpens.put(pkg, openModules);
      }
      instrumentation.redefineModule(
          module,
          Collections.emptySet(),
          Collections.emptyMap(),
          addOpens,
          Collections.emptySet(),
          Collections.emptyMap());
    }
  }
}
