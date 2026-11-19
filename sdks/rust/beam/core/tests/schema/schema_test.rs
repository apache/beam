/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use beam::schema::{AtomicType, Field, FieldType, Schema, TypeInfo};

#[test]
fn test_schema_builder_and_accessors() {
    let schema = Schema::builder()
        .field("id", FieldType::int64())
        .field("name", FieldType::string())
        .nullable_field("age", FieldType::int32())
        .id("test-schema")
        .build();

    assert_eq!(schema.num_fields(), 3);
    assert_eq!(schema.field_index("id"), Some(0));
    assert_eq!(schema.field_index("name"), Some(1));
    assert_eq!(schema.field_index("age"), Some(2));
    assert_eq!(schema.field_index("unknown"), None);

    assert!(!schema.fields[0].field_type.nullable);
    assert!(schema.fields[2].field_type.nullable);
}

#[test]
fn test_schema_display() {
    assert_eq!(FieldType::int64().to_string(), "INT64");
    assert_eq!(
        FieldType::nullable_atomic(AtomicType::String).to_string(),
        "STRING?"
    );
    assert_eq!(
        FieldType::array(FieldType::int32()).to_string(),
        "ARRAY<INT32>"
    );
    assert_eq!(
        FieldType::map(FieldType::string(), FieldType::double()).to_string(),
        "MAP<STRING, DOUBLE>"
    );
}

#[test]
fn schema_round_trips_through_proto_bytes() {
    let nested = Schema::new(vec![Field::new("x", FieldType::double())]);
    let schema = Schema {
        id: Some("events".to_string()),
        ..Schema::new(vec![
            Field::new("id", FieldType::int64()).with_encoding_position(0),
            Field::nullable("tags", FieldType::array(FieldType::string()))
                .with_description("labels")
                .with_encoding_position(1),
            Field::new(
                "attrs",
                FieldType::map(FieldType::string(), FieldType::bytes()),
            )
            .with_encoding_position(2),
            Field::new("point", FieldType::row(nested)).with_encoding_position(3),
            Field::new(
                "ts",
                FieldType::logical(
                    "beam:logical_type:micros_instant:v1",
                    Vec::new(),
                    FieldType::int64(),
                ),
            )
            .with_encoding_position(4),
            Field::new(
                "seq",
                FieldType::new(TypeInfo::Iterable(Box::new(FieldType::int16())), false),
            )
            .with_encoding_position(5),
        ])
    };
    assert!(schema.encoding_positions_set);

    let bytes = schema.to_proto_bytes();
    let decoded = Schema::from_proto_bytes(&bytes).expect("schema proto must decode");
    assert_eq!(decoded, schema);
}

#[test]
fn encoding_positions_require_every_field_and_positions_set_flag() {
    use beam_model::pipeline as proto;

    assert!(!Schema::new(Vec::new()).encoding_positions_set);

    let partial = Schema::new(vec![
        Field::new("a", FieldType::int64()).with_encoding_position(1),
        Field::new("b", FieldType::int64()),
    ]);
    assert!(!partial.encoding_positions_set);
    let partial_round_trip =
        Schema::from_proto_bytes(&partial.to_proto_bytes()).expect("schema proto must decode");
    assert!(!partial_round_trip.encoding_positions_set);
    assert_eq!(partial_round_trip.fields[0].encoding_position, None);
    assert_eq!(partial_round_trip.fields[1].encoding_position, None);

    let mut unflagged =
        proto::Schema::from(&Schema::new(vec![Field::new("a", FieldType::int64())]));
    unflagged.encoding_positions_set = false;
    unflagged.fields[0].encoding_position = 3;
    let decoded = Schema::try_from(unflagged).expect("proto schema must convert");
    assert!(!decoded.encoding_positions_set);
    assert_eq!(decoded.fields[0].encoding_position, None);
}
