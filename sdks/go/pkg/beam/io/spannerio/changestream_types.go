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
	"time"

	"cloud.google.com/go/spanner/apiv1/spannerpb"
)

// ModType describes the kind of mutation in a DataChangeRecord.
type ModType int32

const (
	ModTypeUnspecified ModType = 0
	ModTypeInsert      ModType = 10
	ModTypeUpdate      ModType = 20
	ModTypeDelete      ModType = 30
)

// ValueCaptureType describes what values are captured in a DataChangeRecord.
type ValueCaptureType int32

const (
	ValueCaptureTypeUnspecified        ValueCaptureType = 0
	ValueCaptureTypeOldAndNewValues    ValueCaptureType = 10
	ValueCaptureTypeNewValues          ValueCaptureType = 20
	ValueCaptureTypeNewRow             ValueCaptureType = 30
	ValueCaptureTypeNewRowAndOldValues ValueCaptureType = 40
)

// ColumnMetadata describes the type and key membership of a column involved in a change.
type ColumnMetadata struct {
	// Name is the column name.
	Name string
	// TypeCode is the Spanner type code of the column (e.g. STRING, INT64, BOOL).
	TypeCode spannerpb.TypeCode
	// ArrayElementTypeCode is the element type code when TypeCode is ARRAY.
	// It is TypeCode_TYPE_CODE_UNSPECIFIED for non-array columns.
	ArrayElementTypeCode spannerpb.TypeCode
	// IsPrimaryKey indicates whether this column is part of the primary key.
	IsPrimaryKey bool
	// OrdinalPosition is the column's position in the table schema.
	OrdinalPosition int64
}

// ModValue holds a single column value from a modification.
type ModValue struct {
	// ColumnName is the name of the modified column.
	ColumnName string
	// Value is the column value encoded as a JSON string using the Spanner JSON
	// value format. Use encoding/json to decode into a concrete Go type.
	Value string
}

// Mod describes all changes to one watched table row.
type Mod struct {
	// Keys holds the primary key column values for the modified row.
	Keys []*ModValue
	// OldValues holds the pre-image of modified columns (empty for INSERTs or
	// when the capture type does not include old values).
	OldValues []*ModValue
	// NewValues holds the post-image of modified columns (empty for DELETEs).
	NewValues []*ModValue
}

// DataChangeRecord describes one or more mutations to a Spanner table, committed
// atomically within a single transaction. This is the primary output element of
// ReadChangeStream.
type DataChangeRecord struct {
	// PartitionToken identifies the change stream partition that produced this record.
	PartitionToken string
	// CommitTimestamp is when the mutations were committed in Spanner.
	CommitTimestamp time.Time
	// RecordSequence is monotonically increasing within a partition for a given
	// commit timestamp and transaction. Use it to order records within a transaction.
	RecordSequence string
	// ServerTransactionID is a globally unique identifier for the transaction.
	ServerTransactionID string
	// IsLastRecordInTransactionInPartition indicates whether this is the final
	// DataChangeRecord for this transaction in this partition.
	IsLastRecordInTransactionInPartition bool
	// Table is the name of the modified table.
	Table string
	// ColumnMetadata describes the columns present in Mods.
	ColumnMetadata []*ColumnMetadata
	// Mods lists the individual row modifications.
	Mods []*Mod
	// ModType is the type of operation (INSERT, UPDATE, DELETE).
	ModType ModType
	// ValueCaptureType indicates what values are present in Mods.
	ValueCaptureType ValueCaptureType
	// NumberOfRecordsInTransaction is the total number of DataChangeRecords for
	// this transaction across all partitions.
	NumberOfRecordsInTransaction int32
	// NumberOfPartitionsInTransaction is the total number of partitions that
	// produced records for this transaction.
	NumberOfPartitionsInTransaction int32
	// TransactionTag is the application-defined tag for the transaction.
	TransactionTag string
	// IsSystemTransaction indicates this is a Spanner-internal transaction (e.g., TTL).
	IsSystemTransaction bool
}
