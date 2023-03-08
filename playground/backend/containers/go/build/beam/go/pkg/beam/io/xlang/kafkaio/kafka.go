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

// Package kafkaio contains cross-language functionality for using Apache Kafka
// (http://kafka.apache.org/). These transforms only work on runners that
// support cross-language transforms.
//
// # Setup
//
// Transforms specified here are cross-language transforms implemented in a
// different SDK (listed below). During pipeline construction, the Go SDK will
// need to connect to an expansion service containing information on these
// transforms in their native SDK.
//
// To use an expansion service, it must be run as a separate process accessible
// during pipeline construction. The address of that process must be passed to
// the transforms in this package.
//
// The version of the expansion service should match the version of the Beam SDK
// being used. For numbered releases of Beam, these expansions services are
// released to the Maven repository as modules. For development versions of
// Beam, it is recommended to build and run it from source using Gradle.
//
// Current supported SDKs, including expansion service modules and reference
// documentation:
//
// Java:
//   - Vendored Module: beam-sdks-java-io-expansion-service
//   - Run via Gradle: ./gradlew :sdks:java:io:expansion-service:runExpansionService
//   - Reference Class: org.apache.beam.sdk.io.kafka.KafkaIO
package kafkaio

// TODO(https://github.com/apache/beam/issues/21000): Implement an API for specifying Kafka type serializers and
// deserializers.

import (
	"reflect"

	"github.com/apache/beam/sdks/v2/go/pkg/beam"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/runtime/xlangx"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/typex"
	"github.com/apache/beam/sdks/v2/go/pkg/beam/core/util/reflectx"
)

func init() {
	beam.RegisterType(reflect.TypeOf((*readPayload)(nil)).Elem())
	beam.RegisterType(reflect.TypeOf((*writePayload)(nil)).Elem())
}

type policy string

const (
	ByteArrayDeserializer = "org.apache.kafka.common.serialization.ByteArrayDeserializer"
	ByteArraySerializer   = "org.apache.kafka.common.serialization.ByteArraySerializer"

	// ProcessingTime is a timestamp policy that assigns processing time to
	// each record. Specifically, this is the timestamp when the record becomes
	// "current" in the reader. Further documentation can be found in Java's
	// KafkaIO documentation.
	ProcessingTime policy = "ProcessingTime"

	// CreateTime is a timestamp policy based on the CREATE_TIME timestamps of
	// kafka records. Requires the records to have a type set to
	// org.apache.kafka.common.record.TimestampTypeCREATE_TIME. Further
	// documentation can be found in Java's KafkaIO documentation.
	CreateTime policy = "CreateTime"

	// LogAppendTime is a timestamp policy that assigns Kafka's log append time
	// (server side ingestion time) to each record. Further documentation can
	// be found in Java's KafkaIO documentation.
	LogAppendTime policy = "LogAppendTime"

	readURN  = "beam:transform:org.apache.beam:kafka_read_without_metadata:v1"
	writeURN = "beam:transform:org.apache.beam:kafka_write:v1"

	serviceGradleTarget = ":sdks:java:io:expansion-service:runExpansionService"
)

var autoStartupAddress string = xlangx.UseAutomatedJavaExpansionService(serviceGradleTarget)

// Read is a cross-language PTransform which reads from Kafka and returns a
// KV pair for each item in the specified Kafka topics. By default, this runs
// as an unbounded transform and outputs keys and values as byte slices.
// These properties can be changed through optional parameters.
//
// Read requires the address for an expansion service for Kafka Read transforms,
// a comma-seperated list of bootstrap server addresses (see the Kafka property
// "bootstrap.servers" for details), and at least one topic to read from.
// If an expansion service address is provided as "", an appropriate expansion
// service will be automatically started; however this is slower than having a
// persistent expansion service running.
//
// Read also accepts optional parameters as readOptions. All optional parameters
// are predefined in this package as functions that return readOption. To set
// an optional parameter, call the function within Read's function signature.
//
// Example of Read with required and optional parameters:
//
//	expansionAddr := "localhost:1234"
//	bootstrapServer := "bootstrap-server:1234"
//	topic := "topic_name"
//	pcol := kafkaio.Read( s, expansionAddr, bootstrapServer, []string{topic},
//	    kafkaio.MaxNumRecords(100), kafkaio.CommitOffsetInFinalize(true))
func Read(s beam.Scope, addr string, servers string, topics []string, opts ...readOption) beam.PCollection {
	s = s.Scope("kafkaio.Read")

	if len(topics) == 0 {
		panic("kafkaio.Read requires at least one topic to read from.")
	}

	if addr == "" {
		addr = autoStartupAddress
	}

	rpl := readPayload{
		ConsumerConfig:    map[string]string{"bootstrap.servers": servers},
		Topics:            topics,
		KeyDeserializer:   ByteArrayDeserializer,
		ValueDeserializer: ByteArrayDeserializer,
		TimestampPolicy:   string(ProcessingTime),
	}
	rcfg := readConfig{
		pl:  &rpl,
		key: reflectx.ByteSlice,
		val: reflectx.ByteSlice,
	}
	for _, opt := range opts {
		opt(&rcfg)
	}

	pl := beam.CrossLanguagePayload(rpl)
	outT := beam.UnnamedOutput(typex.NewKV(typex.New(rcfg.key), typex.New(rcfg.val)))
	out := beam.CrossLanguage(s, readURN, pl, addr, nil, outT)
	return out[beam.UnnamedOutputTag()]
}

type readOption func(*readConfig)
type readConfig struct {
	pl  *readPayload
	key reflect.Type
	val reflect.Type
}

// ConsumerConfigs is a Read option that adds consumer properties to the
// Consumer configuration of the transform. Each usage of this adds the given
// elements to the existing map without removing existing elements.
//
// Note that the "bootstrap.servers" property is automatically set by
// kafkaio.Read and does not need to be specified via this option.
func ConsumerConfigs(cfgs map[string]string) readOption {
	return func(cfg *readConfig) {
		for k, v := range cfgs {
			cfg.pl.ConsumerConfig[k] = v
		}
	}
}

// StartReadTimestamp is a Read option that specifies a start timestamp in
// milliseconds epoch, so only records after that timestamp will be read.
//
// This results in failures if one or more partitions don't contain messages
// with a timestamp larger than or equal to the one specified, or if the
// message format version in a partition is before 0.10.0, meaning messages do
// not have timestamps.
func StartReadTimestamp(ts int64) readOption {
	return func(cfg *readConfig) {
		cfg.pl.StartReadTime = &ts
	}
}

// MaxNumRecords is a Read option that specifies the maximum amount of records
// to be read. Setting this will cause the Read to execute as a bounded
// transform. Useful for tests tests and demo applications.
func MaxNumRecords(num int64) readOption {
	return func(cfg *readConfig) {
		cfg.pl.MaxNumRecords = &num
	}
}

// MaxReadSecs is a Read option that specifies the maximum amount of time in
// seconds the transform executes. Setting this will cause the Read to execute
// as a bounded transform. Useful for tests and demo applications.
func MaxReadSecs(secs int64) readOption {
	return func(cfg *readConfig) {
		cfg.pl.MaxReadTime = &secs
	}
}

// CommitOffsetInFinalize is a Read option that specifies whether to commit
// offsets when finalizing.
//
// Default: false
func CommitOffsetInFinalize(enabled bool) readOption {
	return func(cfg *readConfig) {
		cfg.pl.CommitOffsetInFinalize = enabled
	}
}

// TimestampPolicy is a Read option that specifies the timestamp policy to use
// for extracting timestamps from the KafkaRecord. Must be one of the predefined
// constant timestamp policies in this package.
//
// Default: kafkaio.ProcessingTime
func TimestampPolicy(name policy) readOption {
	return func(cfg *readConfig) {
		cfg.pl.TimestampPolicy = string(name)
	}
}

// readPayload should produce a schema matching the expected cross-language
// payload for Kafka reads. An example of this on the receiving end can be
// found in the Java SDK class
// org.apache.beam.sdk.io.kafka.KafkaIO.Read.External.Configuration.
type readPayload struct {
	ConsumerConfig         map[string]string
	Topics                 []string
	KeyDeserializer        string
	ValueDeserializer      string
	StartReadTime          *int64
	MaxNumRecords          *int64
	MaxReadTime            *int64
	CommitOffsetInFinalize bool
	TimestampPolicy        string
}

// Write is a cross-language PTransform which writes KV data to a specified
// Kafka topic. By default, this assumes keys and values to be received as
// byte slices. This can be changed through optional parameters.
//
// Write requires the address for an expansion service for Kafka Write
// transforms, a comma-seperated list of bootstrap server addresses (see the
// Kafka property "bootstrap.servers" for details), and a topic to write to.
// If an expansion service address is provided as "", an appropriate expansion
// service will be automatically started; however this is slower than having a
// persistent expansion service running.
//
// Write also accepts optional parameters as writeOptions. All optional
// parameters are predefined in this package as functions that return
// writeOption. To set an optional parameter, call the function within Write's
// function signature.
//
// Example of Write with required and optional parameters:
//
//	expansionAddr := "localhost:1234"
//	bootstrapServer := "bootstrap-server:1234"
//	topic := "topic_name"
//	pcol := kafkaio.Read(s, expansionAddr, bootstrapServer, topic,
//	    kafkaio.ValueSerializer("foo.BarSerializer"))
func Write(s beam.Scope, addr, servers, topic string, col beam.PCollection, opts ...writeOption) {
	s = s.Scope("kafkaio.Write")

	if addr == "" {
		addr = autoStartupAddress
	}

	wpl := writePayload{
		ProducerConfig:  map[string]string{"bootstrap.servers": servers},
		Topic:           topic,
		KeySerializer:   ByteArraySerializer,
		ValueSerializer: ByteArraySerializer,
	}
	for _, opt := range opts {
		opt(&wpl)
	}

	pl := beam.CrossLanguagePayload(wpl)
	beam.CrossLanguage(s, writeURN, pl, addr, beam.UnnamedInput(col), nil)
}

type writeOption func(*writePayload)

// ProducerConfigs is a Write option that adds producer properties to the
// Producer configuration of the transform. Each usage of this adds the given
// elements to the existing map without removing existing elements.
//
// Note that the "bootstrap.servers" property is automatically set by
// kafkaio.Write and does not need to be specified via this option.
func ProducerConfigs(cfgs map[string]string) writeOption {
	return func(pl *writePayload) {
		for k, v := range cfgs {
			pl.ProducerConfig[k] = v
		}
	}
}

// writePayload should produce a schema matching the expected cross-language
// payload for Kafka writes. An example of this on the receiving end can be
// found in the Java SDK class
// org.apache.beam.sdk.io.kafka.KafkaIO.Write.External.Configuration.
type writePayload struct {
	ProducerConfig  map[string]string
	Topic           string
	KeySerializer   string
	ValueSerializer string
}
