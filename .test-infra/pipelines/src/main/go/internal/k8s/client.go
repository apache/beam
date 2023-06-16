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

package k8s

import (
	"fmt"

	"k8s.io/client-go/rest"
	k8s "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
)

// Client creates various workloads within a Kubernetes environment.
type Client struct {
	internal k8s.Client
}

// NewDefaultClient instantiates a Client from a local kube configuration.
func NewDefaultClient() (*Client, error) {
	return New(config.GetConfigOrDie(), k8s.Options{})
}

// New instantiates a Client from rest.Config and k8s.Options.
func New(config *rest.Config, options k8s.Options) (*Client, error) {
	internal, err := k8s.New(config, options)
	if err != nil {
		return nil, fmt.Errorf("error instantiating kubernetes client: %w", err)
	}
	return &Client{
		internal: internal,
	}, nil
}
