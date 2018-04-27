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

// Package datastoreio provides transformations and utilities to interact with
// Google Datastore. See also: https://cloud.google.com/datastore/docs.
package datastoreio

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"cloud.google.com/go/datastore"
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/core/runtime"
	"github.com/apache/beam/sdks/go/pkg/beam/log"
	"google.golang.org/api/iterator"
)

const (
	scatterPropertyName = "__scatter__"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*queryFn)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*splitQueryFn)(nil)).Elem())
}

// Read reads all rows from the given kind. The kind must have a schema  compatible with the given type, t, and Read
// returns a PCollection<t>. You must also register your type with runtime.RegisterType which allows you to implement
// datastore.PropertyLoadSaver
//
//
// Example:
// type Item struct {}
// itemKey = runtime.RegisterType(reflect.TypeOf((*Item)(nil)).Elem())
//
// datastoreio.Read(s, "project", "Item", 256, reflect.TypeOf(Item{}), itemKey)
func Read(s beam.Scope, project, kind string, shards int, t reflect.Type, typeKey string) beam.PCollection {
	s = s.Scope("datastore.Read")
	return query(s, project, kind, shards, t, typeKey)
}

func query(s beam.Scope, project, kind string, shards int, t reflect.Type, typeKey string) beam.PCollection {
	imp := beam.Impulse(s)
	ex := beam.ParDo(s, &splitQueryFn{Project: project, Kind: kind, Shards: shards}, imp)
	g := beam.GroupByKey(s, ex)
	return beam.ParDo(s, &queryFn{Project: project, Kind: kind, Type: typeKey}, g, beam.TypeDefinition{Var: beam.XType, T: t})
}

type splitQueryFn struct {
	Project string `json:"project"`
	Kind    string `json:"kind"`
	Shards  int    `json:"shards"`
}

// BoundedQuery represents a datastore Query with a bounded key range between [Start, End)
type BoundedQuery struct {
	Start *datastore.Key `json:"start"`
	End   *datastore.Key `json:"end"`
}

func (s *splitQueryFn) ProcessElement(ctx context.Context, _ []byte, emit func(k string, val string)) error {
	// Short circuit a single shard
	if s.Shards <= 1 {
		q := BoundedQuery{}
		b, err := json.Marshal(q)
		if err != nil {
			return err
		}
		emit(strconv.Itoa(1), string(b))
		return nil
	}

	client, err := datastore.NewClient(ctx, s.Project)
	if err != nil {
		return err
	}
	defer client.Close()

	splits := []*datastore.Key{}
	iter := client.Run(ctx, datastore.NewQuery(s.Kind).Order(scatterPropertyName).Limit((s.Shards-1)*32).KeysOnly())
	for {
		k, err := iter.Next(nil)
		if err != nil {
			if err == iterator.Done {
				break
			}
			return err
		}
		splits = append(splits, k)
	}
	sort.Slice(splits, func(i, j int) bool {
		return keyLessThan(splits[i], splits[j])
	})

	splitKeys := getSplits(splits, s.Shards-1)

	queries := make([]*BoundedQuery, len(splitKeys))
	var lastKey *datastore.Key
	for n, k := range splitKeys {
		q := BoundedQuery{End: k}
		if lastKey != nil {
			q.Start = lastKey
		}
		queries[n] = &q
		lastKey = k
	}
	queries = append(queries, &BoundedQuery{Start: lastKey})

	log.Debugf(ctx, "Datastore: Splitting into %d shards", len(queries))

	for n, q := range queries {
		b, err := json.Marshal(q)
		if err != nil {
			return err
		}
		log.Debugf(ctx, "Datastore: Emitting Bounded Query Shard `%d` Start: `%s` End:`%s`", n, q.Start.String(), q.End.String())
		emit(strconv.Itoa(n), string(b))
	}
	return nil
}

func keyLessThan(a *datastore.Key, b *datastore.Key) bool {
	af, bf := flatten(a), flatten(b)
	for n, k1 := range af {
		if n >= len(bf) {
			return true
		}
		k2 := bf[n]
		r := strings.Compare(k1.Name, k2.Name)
		if r == -1 {
			return true
		} else if r == 1 {
			return false
		}
	}
	return false
}

func flatten(k *datastore.Key) []*datastore.Key {
	pieces := []*datastore.Key{}
	if k.Parent != nil {
		pieces = append(pieces, flatten(k.Parent)...)
	}
	pieces = append(pieces, k)
	return pieces
}

func getSplits(keys []*datastore.Key, numSplits int) []*datastore.Key {
	if len(keys) == 0 || (len(keys) < (numSplits - 1)) {
		return keys
	}

	numKeysPerSplit := math.Max(1.0, float64(len(keys))) / float64((numSplits))

	splitKeys := make([]*datastore.Key, numSplits)
	for n := 1; n <= len(splitKeys); n++ {
		i := int(math.Round(float64(n) * float64(numKeysPerSplit)))
		splitKeys[n-1] = keys[i-1]
	}
	return splitKeys

}

type queryFn struct {
	// Project is the project
	Project string `json:"project"`
	// Kind is the datastore kind
	Kind string `json:"kind"`
	// Type is the name of the global schema type
	Type string `json:"type"`
}

func (f *queryFn) ProcessElement(ctx context.Context, _ string, v func(*string) bool, emit func(beam.X)) error {

	client, err := datastore.NewClient(ctx, f.Project)
	if err != nil {
		return err
	}
	defer client.Close()

	// deserialize Query
	var k string
	v(&k)
	q := BoundedQuery{}
	err = json.Unmarshal([]byte(k), &q)
	if err != nil {
		return err
	}

	// lookup type
	t, ok := runtime.LookupType(f.Type)
	if !ok {
		fmt.Errorf("No type registered %s", f.Type)
	}

	// Translate BoundedQuery to datastore.Query
	dq := datastore.NewQuery(f.Kind)
	if q.Start != nil {
		dq = dq.Filter("__key__ >=", q.Start)
	}
	if q.End != nil {
		dq = dq.Filter("__key__ <", q.End)
	}

	// Run Query
	iter := client.Run(ctx, dq)
	for {
		val := reflect.New(t).Interface() // val : *T
		if _, err := iter.Next(val); err != nil {
			if err == iterator.Done {
				break
			}
			return err
		}
		emit(reflect.ValueOf(val).Elem().Interface()) // emit(*val)
	}
	return nil
}
