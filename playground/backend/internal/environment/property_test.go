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

package environment

import "testing"

func TestNew(t *testing.T) {
	tests := []struct {
		name         string
		appPropsPath string
		wantErr      bool
	}{
		{
			name:         "Property constructor: when file path is not valid",
			appPropsPath: "../.",
			wantErr:      true,
		},
		{
			name:         "Property constructor: in the usual case",
			appPropsPath: "../../.",
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			props, err := NewProperties(tt.appPropsPath)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewProperties() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if props.Salt != "Beam playground salt" ||
				props.MaxSnippetSize != 1000000 ||
				props.IdLength != 11 ||
				props.RemovingUnusedSnptsCron != "0 0 0 1 */1 *" {
				t.Errorf("NewProperties(): unexpected result")
			}
		})
	}
}
