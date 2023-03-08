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

// Package funcx contains functions and types used to perform type analysis
// of Beam functions. This package is primarily used in pipeline construction
// to typecheck pipelines, but could be used by anyone who wants to perform
// type analysis of Beam user functions. For performing typechecking of code,
// consider the methods Try* in the beam package, as they are
// built on these primitives and may be more useful.
package funcx
