// Licensed to the Apache Software Foundation (ASF) under one or more
// contributor license agreements.  See the NOTICE file distributed with
// this work for additional information regarding copyright ownership.
// The ASF licenses this file to You under the Apache License, Version 2.0
// (the "License"); you may not use this file except in compliance with
// the License.  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

// Methods are deliberately declared before structs.

// Private methods on private struct -- Ignored.

func (p privateStruct2)                 privateMethodOnPrivateStruct() {}
func (p privateStruct2[int])            privateMethodOnPrivateStructGeneric1() {}
func (p privateStruct2[int, string])    privateMethodOnPrivateStructGeneric2() {}
func (p *privateStruct2)                privateMethodOnPrivateStructPointer() {}
func (p *privateStruct2[int])           privateMethodOnPrivateStructPointerGeneric1() {}
func (p *privateStruct2[int, string])   privateMethodOnPrivateStructPointerGeneric2() {}

// Public methods on private struct -- Ignored.

func (p privateStruct2)                 PublicMethodOnPrivateStruct() {}
func (p privateStruct2[int])            PublicMethodOnPrivateStructGeneric1() {}
func (p privateStruct2[int, string])    PublicMethodOnPrivateStructGeneric2() {}
func (p *privateStruct2)                PublicMethodOnPrivateStructPointer() {}
func (p *privateStruct2[int])           PublicMethodOnPrivateStructPointerGeneric1() {}
func (p *privateStruct2[int, string])   PublicMethodOnPrivateStructPointerGeneric2() {}

// Private methods on public struct -- Ignored.

func (p PublicStruct2)                  privateMethodOnPublicStruct() {}
func (p PublicStruct2[int])             privateMethodOnPublicStructGeneric1() {}
func (p PublicStruct2[int, string])     privateMethodOnPublicStructGeneric2() {}
func (p *PublicStruct2)                 privateMethodOnPublicStructPointer() {}
func (p *PublicStruct2[int])            privateMethodOnPublicStructPointerGeneric1() {}
func (p *PublicStruct2[int, string])    privateMethodOnPublicStructPointerGeneric2() {}

// Public methods on public struct.

func (p PublicStruct2)                  PublicMethodOnPublicStruct() {}
func (p PublicStruct2[int])             PublicMethodOnPublicStructGeneric1() {}
func (p PublicStruct2[int, string])     PublicMethodOnPublicStructGeneric2() {}
func (p *PublicStruct2)                 PublicMethodOnPublicStructPointer() {}
func (p *PublicStruct2[int])            PublicMethodOnPublicStructPointerGeneric1() {}
func (p *PublicStruct2[int, string])    PublicMethodOnPublicStructPointerGeneric2() {}

// Structs

type privateStruct1 struct {
    privateField1 int
    PublicField2 *string
}

type PublicStruct2 struct {
    privateField1 int
    PublicField2 *string
}

type PublicStruct3 struct {
    PublicStruct2
    PublicField3 int
}

func (p PublicStruct2) MethodAfterStructDeclaration() {}
