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

package reflectx

import (
	"encoding/json"
	"reflect"

	"github.com/apache/beam/sdks/go/pkg/beam/internal/errors"
)

// UnmarshalJSON decodes a json string as then given type. It is suitable
// for when the type is not statically known.
func UnmarshalJSON(t reflect.Type, str string) (interface{}, error) {
	data := reflect.New(t).Interface()
	if err := json.Unmarshal([]byte(str), data); err != nil {
		return nil, errors.Wrap(err, "failed to decode data")
	}
	return reflect.ValueOf(data).Elem().Interface(), nil
}
