// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Integration tests for predicate evaluation with Reference and Transform terms.

use std::sync::Arc;

use arrow_array::Int64Array;
use futures::TryStreamExt;
use iceberg::expr::{Reference, TransformTerm};
use iceberg::spec::Datum;
use iceberg::{Catalog, CatalogBuilder, TableIdent};
use iceberg_catalog_rest::RestCatalogBuilder;
use iceberg_integration_tests::get_test_fixture;
use iceberg_storage_opendal::OpenDalStorageFactory;

#[tokio::test]
async fn test_predicate_with_reference_term() {
    let fixture = get_test_fixture();
    let rest_catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let table = rest_catalog
        .load_table(&TableIdent::from_strs(["default", "test_promote_column"]).unwrap())
        .await
        .unwrap();

    // Reference term predicate: foo != 22
    let predicate = Reference::new("foo").not_equal_to(Datum::int(22));

    let scan = table.scan().with_filter(predicate).build();
    let batch_stream = scan.unwrap().to_arrow().await.unwrap();

    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

    let mut actual = vec![
        batches[0]
            .column_by_name("foo")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        batches[1]
            .column_by_name("foo")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
    ];

    actual.sort();

    assert_eq!(actual, vec![19, 25]);
}

#[tokio::test]
async fn test_predicate_with_transform_term() {
    let fixture = get_test_fixture();
    let rest_catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            customized_credential_load: None,
        }))
        .load("rest", fixture.catalog_config.clone())
        .await
        .unwrap();

    let table = rest_catalog
        .load_table(
            &TableIdent::from_strs(["default", "test_promote_partition_column"]).unwrap(),
        )
        .await
        .unwrap();

    // Transform term predicate: bucket(16, foo) = 3
    // This tests that TransformTerm can be constructed and passed through the scan pipeline.
    // The predicate will be bound to the table schema, then projected via InclusiveProjection
    // to a partition-field predicate.
    let predicate = TransformTerm::bucket("foo", 16).equal_to(Datum::int(3));

    let scan = table.scan().with_filter(predicate).build();
    let batch_stream = scan.unwrap().to_arrow().await.unwrap();

    let batches: Vec<_> = batch_stream.try_collect().await.unwrap();

    // Verify we get the expected data back (same as unfiltered scan for this fixture)
    let mut actual = vec![
        batches[0]
            .column_by_name("foo")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        batches[1]
            .column_by_name("foo")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
    ];

    actual.sort();

    assert_eq!(actual, vec![19, 25]);
}
