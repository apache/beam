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
package org.apache.beam.sdk.schemas.utils;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.description.type.TypeDescription.ForLoadedType;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.StackManipulation;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.StackManipulation.Compound;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.assign.TypeCasting;
import org.apache.beam.vendor.bytebuddy.v1_10_8.net.bytebuddy.implementation.bytecode.member.MethodVariableAccess;

/** This class allows managing local variables in a ByteBuddy-generated function. */
class ByteBuddyLocalVariableManager {
  private int nextLocalVariableIndex;

  // Initialize with the number of arguments to the function (including the this parameter if
  // applicable).
  public ByteBuddyLocalVariableManager(int numFunctionArgs) {
    nextLocalVariableIndex = numFunctionArgs;
  }

  // Create a new variable.
  public int createVariable() {
    return nextLocalVariableIndex++;
  }

  // Returns a StackManipulation to read a variable.
  public StackManipulation readVariable(int variableIndex) {
    checkArgument(variableIndex < nextLocalVariableIndex);
    return MethodVariableAccess.REFERENCE.loadFrom(variableIndex);
  }

  // Returns a StackManipulation to read a variable, casting to the specified type.
  StackManipulation readVariable(int variableIndex, Class<?> type) {
    return new Compound(readVariable(variableIndex), TypeCasting.to(new ForLoadedType(type)));
  }

  // Returns a StackManipulation to write a variable.
  StackManipulation writeVariable(int variableIndex) {
    checkArgument(variableIndex < nextLocalVariableIndex);
    return MethodVariableAccess.REFERENCE.storeAt(variableIndex);
  }

  // Returns a StackManipulation to copy a variable.
  StackManipulation copy(int sourceVariableIndex, int destVariableIndex) {
    return new Compound(readVariable(sourceVariableIndex), writeVariable(destVariableIndex));
  }

  // Returns a class that can be used to backup and restore a variable, using a newly-created temp
  // variable.
  BackupLocalVariable backupVariable(int variableToBackup) {
    return new BackupLocalVariable(variableToBackup);
  }

  // Gets the total num variables in the function. Should be used when returning the Size parameter.
  int getTotalNumVariables() {
    return nextLocalVariableIndex;
  }

  public class BackupLocalVariable {
    private final int variableToBackup; // Variable to save.
    private final int tempVariable; // Temp variable we are saving in.

    public BackupLocalVariable(int variableToBackup) {
      this.variableToBackup = variableToBackup;
      this.tempVariable = createVariable();
    }

    public StackManipulation backup() {
      return copy(variableToBackup, tempVariable);
    }

    public StackManipulation restore() {
      return copy(tempVariable, variableToBackup);
    }
  }
}
