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

import "github.com/spf13/viper"

const (
	//configName is the name for the config file.
	configName = "properties"
	//configType is the type of the configuration returned by the remote source, e.g. "json", "yaml", "env" and so on.
	configType = "yaml"
)

// Properties contains all properties that needed to run backend processes.
type Properties struct {
	// Salt is the salt to generate the hash to avoid whatever problems a collision may cause.
	Salt string `mapstructure:"playground_salt"`
	// MaxSnippetSize is the file content size limit. Since 1 character occupies 1 byte of memory, and 1 MB is approximately equal to 1000000 bytes, then maximum size of the snippet is 1000000.
	MaxSnippetSize int32 `mapstructure:"max_snippet_size"`
	// IdLength is the length of the identifier that is used to store data in the cloud datastore. It's appropriate length to save storage size in the cloud datastore and provide good randomnicity.
	IdLength int8 `mapstructure:"id_length"`
	// RemovingUnusedSnptsCron is the cron expression for the scheduled task to remove unused snippets
	RemovingUnusedSnptsCron string `mapstructure:"removing_unused_snippets_cron"`
}

func NewProperties(configPath string) (*Properties, error) {
	viper.AddConfigPath(configPath)
	viper.SetConfigName(configName)
	viper.SetConfigType(configType)
	if err := viper.ReadInConfig(); err != nil {
		return nil, err
	}
	properties := new(Properties)
	if err := viper.Unmarshal(properties); err != nil {
		return nil, err
	}
	return properties, nil
}
