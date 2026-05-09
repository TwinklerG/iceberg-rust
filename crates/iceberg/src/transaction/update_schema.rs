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

use std::sync::Arc;

use async_trait::async_trait;

use crate::Result;
use crate::spec::update::{SchemaOperation, schema_update};
use crate::table::Table;
use crate::transaction::{ActionCommit, TransactionAction};

pub struct UpdateSchemaAction {
    operations: Vec<SchemaOperation>,
}

impl UpdateSchemaAction {
    pub fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }
}

impl Default for UpdateSchemaAction {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl TransactionAction for UpdateSchemaAction {
    async fn commit(self: Arc<Self>, _table: &Table) -> Result<ActionCommit> {
        let schema = schema_update(_table.current_schema_ref(), &self.operations);
        Ok(ActionCommit::new(vec![], vec![]))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::LazyLock;

    use crate::spec::{ListType, MapType, NestedField, PrimitiveType, Schema, StructType, Type};

    static SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
        Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "data", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(
                    3,
                    "preferences",
                    Type::Struct(StructType::new(vec![
                        NestedField::required(
                            8,
                            "feature1",
                            Type::Primitive(PrimitiveType::Boolean),
                        )
                        .into(),
                        NestedField::optional(
                            9,
                            "feature2",
                            Type::Primitive(PrimitiveType::Boolean),
                        )
                        .into(),
                    ])),
                )
                .with_doc("struct of named boolean options")
                .into(),
                NestedField::required(
                    4,
                    "locations",
                    Type::Map(MapType::new(
                        NestedField::map_key_element(
                            10,
                            Type::Struct(StructType::new(vec![
                                NestedField::required(
                                    20,
                                    "address",
                                    Type::Primitive(PrimitiveType::String),
                                )
                                .into(),
                                NestedField::required(
                                    21,
                                    "city",
                                    Type::Primitive(PrimitiveType::String),
                                )
                                .into(),
                                NestedField::required(
                                    22,
                                    "state",
                                    Type::Primitive(PrimitiveType::String),
                                )
                                .into(),
                                NestedField::required(
                                    23,
                                    "zip",
                                    Type::Primitive(PrimitiveType::Int),
                                )
                                .into(),
                            ])),
                        )
                        .into(),
                        NestedField::map_value_element(
                            11,
                            Type::Struct(StructType::new(vec![
                                NestedField::required(
                                    12,
                                    "lat",
                                    Type::Primitive(PrimitiveType::Float),
                                )
                                .into(),
                                NestedField::required(
                                    13,
                                    "long",
                                    Type::Primitive(PrimitiveType::Float),
                                )
                                .into(),
                            ])),
                            true,
                        )
                        .into(),
                    )),
                )
                .with_doc("map of address to coordinate")
                .into(),
                NestedField::optional(
                    5,
                    "points",
                    Type::List(ListType::new(
                        NestedField::optional(
                            14,
                            "",
                            Type::Struct(StructType::new(vec![
                                NestedField::required(
                                    15,
                                    "x",
                                    Type::Primitive(PrimitiveType::Long),
                                )
                                .into(),
                                NestedField::required(
                                    16,
                                    "y",
                                    Type::Primitive(PrimitiveType::Long),
                                )
                                .into(),
                            ])),
                        )
                        .into(),
                    )),
                )
                .with_doc("2-D cartesian points")
                .into(),
                NestedField::required(
                    6,
                    "doubles",
                    Type::List(ListType::new(
                        NestedField::required(
                            17,
                            "element",
                            Type::Primitive(PrimitiveType::Double),
                        )
                        .into(),
                    )),
                )
                .into(),
                NestedField::optional(
                    7,
                    "properties",
                    Type::Map(MapType::required(
                        18,
                        Type::Primitive(PrimitiveType::String),
                        19,
                        Type::Primitive(PrimitiveType::String),
                    )),
                )
                .with_doc("string map of properties")
                .into(),
            ])
            .build()
            .unwrap()
    });

    #[test]
    fn test_no_changes() {
        // let identical =
    }
}
