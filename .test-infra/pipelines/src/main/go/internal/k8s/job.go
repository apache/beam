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
	"context"
	"fmt"
	"strings"

	"github.com/apache/beam/test-infra/pipelines/src/main/go/internal/environment"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8s "sigs.k8s.io/controller-runtime/pkg/client"
)

// Jobs creates Kubernetes jobs.
type Jobs struct {
	namespace *Namespace
	internal  k8s.Client
}

// Spec configures a Kubernetes job.
type Spec struct {
	Name          string
	ContainerName string
	Image         string
	Labels        map[string]string
	Command       []string
	RestartOnFail bool
	Environment   []environment.Variable
}

// Jobs instantiates a Jobs within a Namespace from a Client.
func (client *Client) Jobs(ns *Namespace) *Jobs {
	return &Jobs{
		namespace: ns,
		internal:  client.internal,
	}
}

func (js *Jobs) init(ctx context.Context, name string) (*batchv1.Job, error) {
	if err := js.namespace.Exists(ctx); err != nil && errors.IsNotFound(err) {
		return nil, fmt.Errorf("namespace: %s does not exist", js.namespace.name)
	}
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: js.namespace.name,
		},
	}, nil
}

// Create a Kubernetes Job, configured with a Spec.
func (js *Jobs) Create(ctx context.Context, spec *Spec, envKV ...string) (*batchv1.Job, error) {
	if len(envKV)%2 != 0 {
		return nil, fmt.Errorf("envKV format invalid, expected key1, value1, key2, value2, ..., got: %s", strings.Join(envKV, ", "))
	}
	job, err := js.init(ctx, spec.Name)
	if err != nil {
		return nil, err
	}

	restartPolicy := corev1.RestartPolicyNever
	if spec.RestartOnFail {
		restartPolicy = corev1.RestartPolicyOnFailure
	}

	var envs []corev1.EnvVar
	for i := 0; i < len(envKV)-1; i += 2 {
		key := envKV[i]
		value := envKV[i+1]
		envs = append(envs, corev1.EnvVar{
			Name:  key,
			Value: value,
		})
	}

	job.Spec = batchv1.JobSpec{
		Selector: &metav1.LabelSelector{
			MatchLabels: spec.Labels,
		},
		Template: corev1.PodTemplateSpec{
			Spec: corev1.PodSpec{
				Containers: []corev1.Container{
					{
						Name:            spec.ContainerName,
						Image:           spec.Image,
						Command:         spec.Command,
						Env:             envs,
						ImagePullPolicy: corev1.PullIfNotPresent,
					},
				},
				RestartPolicy: restartPolicy,
			},
		},
	}

	if err := js.internal.Create(ctx, job); err != nil {
		return nil, err
	}

	return job, nil
}

func (js *Jobs) Delete(ctx context.Context, name string) error {
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: js.namespace.name,
			Name:      name,
		},
	}

	return js.internal.Delete(ctx, job)
}

func (js *Jobs) Describe(ctx context.Context, name string) (*batchv1.Job, error) {
	job := &batchv1.Job{}
	if err := js.internal.Get(ctx, k8s.ObjectKey{
		Namespace: js.namespace.name,
		Name:      name,
	}, job); err != nil {
		return nil, err
	}
	return job, nil
}

func (js *Jobs) List(ctx context.Context) ([]*batchv1.Job, error) {
	var result []*batchv1.Job
	jobs := &batchv1.JobList{}
	if err := js.internal.List(ctx, jobs, k8s.InNamespace(js.namespace.name)); err != nil {
		return nil, err
	}
	for _, k := range jobs.Items {
		job, err := js.init(ctx, k.GetName())
		if err != nil {
			return nil, err
		}
		result = append(result, job)
	}
	return result, nil
}
