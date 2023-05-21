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

// Package fhirio provides an API for reading and writing resources to Google
// Cloud Healthcare Fhir stores.
package fhirio

import (
	"context"
	"encoding/json"
	"net/url"
	"strings"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/internal/errors"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/log"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/register"
)

func init() {
	register.DoFn4x0[context.Context, SearchQuery, func(string, []string), func(string)]((*searchResourcesFn)(nil))
	register.Emitter1[string]()
	register.Emitter2[string, []string]()
}

// SearchQuery concisely represents a FHIR search query, and should be used as
// input type for the Search transform.
type SearchQuery struct {
	// An identifier for the query, if there is source information to propagate
	// through the pipeline.
	Identifier string
	// Search will be performed only on resources of type ResourceType. If not set
	// (i.e. ""), the search is performed across all resources.
	ResourceType string
	// Query parameters for a FHIR search request as per https://www.hl7.org/fhir/search.html.
	Parameters map[string]string
}

type responseLinkFields struct {
	Relation string `json:"relation"`
	Url      string `json:"url"`
}

type searchResourcesFn struct {
	fnCommonVariables
	// Path to FHIR store where search will be performed.
	FhirStorePath string
}

func (fn searchResourcesFn) String() string {
	return "searchResourcesFn"
}

func (fn *searchResourcesFn) Setup() {
	fn.fnCommonVariables.setup(fn.String())
}

func (fn *searchResourcesFn) ProcessElement(ctx context.Context, query SearchQuery, emitFoundResources func(string, []string), emitDeadLetter func(string)) {
	resourcesFound, err := executeAndRecordLatency(ctx, &fn.latencyMs, func() ([]string, error) {
		return fn.searchResources(ctx, query)
	})
	if err != nil {
		fn.resourcesErrorCount.Inc(ctx, 1)
		emitDeadLetter(errors.Wrapf(err, "error occurred while performing search for query: [%v]", query).Error())
		return
	}

	fn.resourcesSuccessCount.Inc(ctx, 1)
	emitFoundResources(query.Identifier, resourcesFound)
}

func (fn *searchResourcesFn) searchResources(ctx context.Context, query SearchQuery) ([]string, error) {
	resourcesInPage, nextPageToken, err := fn.searchResourcesPaginated(ctx, query, "")
	allResources := resourcesInPage
	for nextPageToken != "" {
		resourcesInPage, nextPageToken, err = fn.searchResourcesPaginated(ctx, query, nextPageToken)
		allResources = append(allResources, resourcesInPage...)
	}
	return allResources, err
}

// Performs a search request retrieving results only from the page identified by
// `pageToken`. If `pageToken` is the empty string it will retrieve the results
// from the first page.
func (fn *searchResourcesFn) searchResourcesPaginated(ctx context.Context, query SearchQuery, pageToken string) ([]string, string, error) {
	response, err := fn.client.search(fn.FhirStorePath, query.ResourceType, query.Parameters, pageToken)
	if err != nil {
		return nil, "", err
	}

	body, err := extractBodyFrom(response)
	if err != nil {
		return nil, "", err
	}

	var bodyFields struct {
		Entries []any                `json:"entry"`
		Links   []responseLinkFields `json:"link"`
	}
	err = json.NewDecoder(strings.NewReader(body)).Decode(&bodyFields)
	if err != nil {
		return nil, "", err
	}

	resourcesFoundInPage := mapEntryToString(ctx, bodyFields.Entries)
	return resourcesFoundInPage, extractNextPageTokenFrom(ctx, bodyFields.Links), nil
}

func mapEntryToString(ctx context.Context, entries []any) []string {
	stringifiedEntries := make([]string, 0)
	for _, entry := range entries {
		entryBytes, err := json.Marshal(entry)
		if err != nil {
			log.Warnf(ctx, "Ignoring malformed entry resource. Error: %v", err)
			continue
		}
		stringifiedEntries = append(stringifiedEntries, string(entryBytes))
	}
	return stringifiedEntries
}

func extractNextPageTokenFrom(ctx context.Context, searchResponseLinks []responseLinkFields) string {
	for _, link := range searchResponseLinks {
		// The link with relation field valued "next" contains the page token
		if link.Relation != "next" {
			continue
		}

		parsedUrl, err := url.Parse(link.Url)
		if err != nil {
			log.Warnf(ctx, "Search next page token failed to be parsed from URL [%v]. Reason: %v", link.Url, err)
			break
		}
		return parsedUrl.Query().Get(pageTokenParameterKey)
	}
	return ""
}

// Search transform searches for resources in a Google Cloud Healthcare FHIR
// store based on input queries. It consumes a PCollection<fhirio.SearchQuery>
// and outputs two PCollections, the first a tuple (identifier, searchResults)
// where `identifier` is the SearchQuery identifier field and `searchResults` is
// a slice of all found resources as a JSON-encoded string. The second
// PCollection is a dead-letter for the input queries that caused errors when
// performing the search.
// See: https://cloud.google.com/healthcare-api/docs/how-tos/fhir-search
func Search(s beam.Scope, fhirStorePath string, searchQueries beam.PCollection) (beam.PCollection, beam.PCollection) {
	s = s.Scope("fhirio.Search")
	return search(s, fhirStorePath, searchQueries, nil)
}

// This is useful as an entry point for testing because we can provide a fake FHIR store client.
func search(s beam.Scope, fhirStorePath string, searchQueries beam.PCollection, client fhirStoreClient) (beam.PCollection, beam.PCollection) {
	return beam.ParDo2(s, &searchResourcesFn{fnCommonVariables: fnCommonVariables{client: client}, FhirStorePath: fhirStorePath}, searchQueries)
}
