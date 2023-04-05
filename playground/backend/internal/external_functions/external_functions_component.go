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

package external_functions

import (
	"beam.apache.org/playground/backend/internal/environment"
	"beam.apache.org/playground/backend/internal/logger"
	"context"
	"fmt"
	"net/http"
)

type ExternalFunctions interface {
	// CleanupSnippets removes old snippets from the database.
	CleanupSnippets(ctx context.Context) error

	// DeleteObsoleteSnippets removes old snippets from the database.
	DeleteObsoleteSnippets(ctx context.Context, snipId string, persistenceKey string) error

	// IncrementSnippetViews increments the number of views for the snippet.
	IncrementSnippetViews(ctx context.Context, snipId string) error
}

type externalFunctionsComponent struct {
	cleanupSnippetsFunctionsUrl        string
	deleteObsoleteSnippetsFunctionsUrl string
	incrementSnippetViewsFunctionsUrl  string
}

func NewExternalFunctionsComponent(appEnvs environment.ApplicationEnvs) ExternalFunctions {
	return &externalFunctionsComponent{
		cleanupSnippetsFunctionsUrl:        appEnvs.CleanupSnippetsFunctionsUrl(),
		deleteObsoleteSnippetsFunctionsUrl: appEnvs.DeleteObsoleteSnippetsFunctionsUrl(),
		incrementSnippetViewsFunctionsUrl:  appEnvs.IncrementSnippetViewsFunctionsUrl(),
	}
}

func makePostRequest(ctx context.Context, requestUrl string) error {
	request, err := http.NewRequestWithContext(ctx, "POST", requestUrl, nil)
	if err != nil {
		logger.Errorf("makePostRequest(): Couldn't create the request, err: %s\n", err.Error())
		return err
	}

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		logger.Errorf("makePostRequest(): Couldn't make POST request to the %s, err: %s\n", requestUrl, err.Error())
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("status code: %d", resp.StatusCode)
	}

	return nil
}

func (c *externalFunctionsComponent) CleanupSnippets(ctx context.Context) error {
	requestUrl := fmt.Sprintf("%s", c.cleanupSnippetsFunctionsUrl)

	if err := makePostRequest(ctx, requestUrl); err != nil {
		logger.Errorf("CleanupSnippets(): Couldn't cleanup snippets, err: %s\n", err.Error())
		return err
	}

	return nil
}

func (c *externalFunctionsComponent) DeleteObsoleteSnippets(ctx context.Context, snipId string, persistenceKey string) error {
	requestUrl := fmt.Sprintf("%s?snipId=%s&persistenceKey=%s", c.deleteObsoleteSnippetsFunctionsUrl, snipId, persistenceKey)

	if err := makePostRequest(ctx, requestUrl); err != nil {
		logger.Errorf("DeleteObsoleteSnippets(): Couldn't delete obsolete snippets, err: %s\n", err.Error())
		return err
	}

	return nil
}

func (c *externalFunctionsComponent) IncrementSnippetViews(ctx context.Context, snipId string) error {
	requestUrl := fmt.Sprintf("%s?snipId=%s", c.incrementSnippetViewsFunctionsUrl, snipId)

	if err := makePostRequest(ctx, requestUrl); err != nil {
		logger.Errorf("IncrementSnippetViews(): Couldn't increment snippet views, err: %s\n", err.Error())
		return err
	}

	return nil
}
