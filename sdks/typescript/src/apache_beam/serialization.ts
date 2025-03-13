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

import * as serialize_closures from "serialize-closures";

const registeredModules: string[] = [];
export const registeredObjects: serialize_closures.BuiltinRecord[] = [];
const registeredObjectSet: Set<any> = new Set();

/**
 * Beam uses ts-serialize-closures in order to serialize objects (including
 * functions with closures) to be sent to remote workers.  This requires
 * serializing the transitive closure of things that may be reached from the
 * root object, which in particular may encounter objects that cannot be
 * serialized (such as classes and closures from libraries that were not
 * compiled with the ts-serialize-closures hook).
 *
 * This function can be used to register "terminal" objects to prevent
 * serialization errors. The module in question will be imported and all its
 * exported members registered. Additional (e.g. private) members to be
 * registered may be passed as well.
 *
 * In addition to registering the objects for serialization at pipeline
 * construction time, the given module will be imported (and their members
 * registered for deserialization) on the remote workers.  If any extra
 * objects are passed, it should be arranged that importing the indicated
 * module will cause this registration call to be executed.
 *
 * Concretely, if I had a module 'myModule' that exported func, I could write
 *
 *   requireForSerialization('myModule');
 *
 * which would then regester `myModule.func` for serialization and cause
 * myModule to be imported on the worker.  If myModule had a private member,
 * say MyClass, that could be exposed externally (e.g. instances of MyClass
 * were returned) one would write
 *
 *   requireForSerialization('myModule', {'MyClass': MyClass});
 *
 * which would allow instances of MyClass to be serialized and, as myModule
 * will also be executed on the worker, this registration code would be run
 * allowing these instances to be deserialized there.
 */
export function requireForSerialization(moduleName: string, values = {}) {
  // TODO: It'd be nice to always validate moduleName, but self imports don't
  // work by default (even if they will on the worker).
  if (!values) {
    values = require(moduleName);
  }
  if (!registeredModules.includes(moduleName)) {
    registeredModules.push(moduleName);
  }
  registerExports(moduleName, values);
}

export function getRegisteredModules() {
  return registeredModules;
}

function registerExports(qualified_name, module) {
  registerItem(qualified_name, module);
  for (const [attr, value] of Object.entries(module)) {
    if (!value) continue;
    registerObject(`${qualified_name}.${attr}`, value);
  }
}

function registerObject(qualified_name, value) {
  for (const [suffix, func] of Object.entries({
    self: (x) => x,
    prototype: (x) => x.prototype,
    "Object.getPrototypeOf": (x) => Object.getPrototypeOf(x),
    constructor: (x) => x.constructor,
  })) {
    const name = `beam:${qualified_name}.${suffix}`;
    try {
      const entry = func(value);
      registerItem(name, entry);
    } catch (err) {}
  }
}

function registerItem(name, entry) {
  if (!entry || typeof entry === "number" || typeof entry === "string") {
    // These already serialize just fine.
    return;
  } else if (registeredObjectSet.has(entry)) {
    // Don't register things twice.
    return;
  } else {
    registeredObjects.push({ name: name, builtin: entry });
  }
}
