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

import "fmt"

// ServerEnvs contains all environment variables that need to run server.
type ServerEnvs struct {
	ip   string
	port int
}

// NewServerEnvs constructor for ServerEnvs
func NewServerEnvs(ip string, port int) *ServerEnvs {
	return &ServerEnvs{ip: ip, port: port}
}

// Address returns concatenated ip and port through ':'
func (serverEnvs ServerEnvs) Address() string {
	return fmt.Sprintf("%s:%d", serverEnvs.ip, serverEnvs.port)
}
