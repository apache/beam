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

package spannerio

import (
	"context"
	"testing"

	"cloud.google.com/go/spanner"
	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/passert"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/testing/ptest"
)

func TestGeneratePartitions(t *testing.T) {
	database := "projects/fake-proj/instances/fake-instance/databases/fake-db-4-rows"

	query := "SELECT * from Test"

	p := beam.NewPipeline()
	s := p.Root()

	fn := newGeneratePartitionsFn(database, query)
	fn.generator = &partitionGeneratorStub{}

	partitions := fn.generatePartitions(s)

	passert.Count(s, partitions, "", 1)
	ptest.RunAndValidate(t, p)
}

type partitionGeneratorStub struct {
}

func (g *partitionGeneratorStub) generate(
	ctx context.Context,
	tb spanner.TimestampBound,
	query string,
	opts spanner.PartitionOptions,
) (spanner.BatchReadOnlyTransactionID, []*spanner.Partition) {
	return spanner.BatchReadOnlyTransactionID{}, []*spanner.Partition{&spanner.Partition{}}
}
