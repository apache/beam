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

package task

import (
	"fmt"
	"github.com/apache/beam/sdks/go/pkg/beam"
)

func ApplyTransform(s beam.Scope, fruits beam.PCollection, countries beam.PCollection) beam.PCollection {
	fruitsKV := beam.ParDo(s, func(word string) (string, string) {
		return string(word[0]), word
	}, fruits)

	countriesKV := beam.ParDo(s, func(word string) (string, string) {
		return string(word[0]), word
	}, countries)

	grouped := beam.CoGroupByKey(s, fruitsKV, countriesKV)
	return beam.ParDo(s, func(key string, fruitsIter func(*string) bool, countriesIter func(*string) bool, emit func(string)) {
		wa := &WordsAlphabet{
			Alphabet: key,
		}
		fruitsIter(&wa.Fruit)
		countriesIter(&wa.Country)
		emit(wa.String())
	}, grouped)
}

type WordsAlphabet struct {
	Alphabet string
	Fruit string
	Country string
}

func (wa *WordsAlphabet) String() string {
	return fmt.Sprintf("WordsAlphabet%+v", *wa)
}

