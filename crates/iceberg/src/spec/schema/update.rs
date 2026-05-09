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

use std::collections::HashMap;
use std::sync::Arc;

use typed_builder::TypedBuilder;

use crate::spec::schema::index::index_parents;
use crate::spec::{
    ListType, Literal, MapType, NestedField, NestedFieldRef, PrimitiveType, Schema, SchemaRef,
    SchemaVisitor, StructType, Type, visit_schema,
};
use crate::{Error, ErrorKind, Result, ensure_precondition};

/// Operations that can be applied to a schema to produce a new schema. These are used in `UpdateSchemaAction` and are not intended to be used directly by end users. Instead, end users should use `UpdateSchema` which will be converted into a list of `SchemaOperation`s.
pub enum SchemaOperation {
    /// Add a column to the schema
    Add(AddColumn),
    /// Update a column's type, doc, or default value
    Update(UpdateColumn),
    /// Rename a column
    Rename(RenameColumn),
    /// Delete a column
    Delete(DeleteColumn),
    /// Move a column to a new position
    Move(MoveColumn),
}

/// A column to be added to the schema. The `parent` field specifies the parent struct column if the column is being added to a nested struct, or is `None` if the column is being added to the top level of the schema. The `name` field specifies the name of the new column. The `is_optional` field specifies whether the new column is optional. The `type` field specifies the type of the new column. The `doc` field specifies an optional doc string for the new column. The `default_value` field specifies an optional default value for the new column.
#[derive(TypedBuilder)]
pub struct AddColumn {
    #[builder(default, setter(strip_option))]
    parent: Option<String>,
    #[builder(setter(into))]
    name: String,
    #[builder(default = true)]
    is_optional: bool,
    r#type: Type,
    #[builder(default, setter(strip_option))]
    doc: Option<Option<String>>,
    #[builder(default, setter(strip_option))]
    default_value: Option<Literal>,
}

/// A column to be deleted from the schema. The `name` field specifies the name of the column to be deleted.
pub struct DeleteColumn {
    name: String,
}

/// A column to be renamed in the schema. The `name` field specifies the current name of the column, and the `new_name` field specifies the new name for the column.
pub struct RenameColumn {
    name: String,
    new_name: String,
}

impl RenameColumn {
    fn new(name: impl Into<String>, new_name: impl Into<String>) -> Self {
        RenameColumn {
            name: name.into(),
            new_name: new_name.into(),
        }
    }
}

/// A column to be updated in the schema. The `name` field specifies the name of the column to be updated. The `new_type` field specifies an optional new type for the column. The `new_doc` field specifies an optional new doc string for the column. The `new_default_value` field specifies an optional new default value for the column.
pub struct UpdateColumn {
    name: String,
    new_type: Option<Type>,
    new_doc: Option<Option<String>>,
    new_default_value: Option<Option<Literal>>,
}

/// A column to be moved in the schema. The `name` field specifies the name of the column to be moved. The `move` field specifies the move operation, which can be either `FIRST`, `BEFORE <reference_column>`, or `AFTER <reference_column>`.
pub struct MoveColumn {
    name: String,
    r#move: Move,
}

const TABLE_ROOT_ID: i32 = -1;

/// Applies a list of `SchemaOperation`s to a `Schema` to produce a new `Schema`. This is used in `UpdateSchemaAction` to apply schema changes as part of a transaction commit. This function validates that the schema operations are valid (e.g. that added columns do not have duplicate names, that deleted columns exist, etc.) and returns an error if any invalid operations are found. If all operations are valid, it returns the updated schema.
pub fn schema_update(schema: SchemaRef, operations: &Vec<SchemaOperation>) -> Result<SchemaRef> {
    let mut updates: HashMap<i32, NestedFieldRef> = HashMap::new();
    let mut deletes = Vec::new();
    let mut moves = HashMap::new();
    let mut parent_to_added_ids = HashMap::new();
    let mut id_to_parent = index_parents(&schema.r#struct).unwrap();
    let mut last_column_id = schema.highest_field_id;
    let mut added_name_to_id = HashMap::new();
    let mut identifier_field_ids = schema.identifier_field_ids.clone();
    for operation in operations {
        match operation {
            SchemaOperation::Add(add) => {
                let (parent, name, is_optional, field_type, doc, default_value) = (
                    &add.parent,
                    &add.name,
                    add.is_optional,
                    &add.r#type,
                    &add.doc,
                    &add.default_value,
                );
                let mut parent_id = TABLE_ROOT_ID;
                let full_name = if let Some(parent) = parent {
                    let parent_field = schema.field_by_name(parent).ok_or(Error::new(
                        ErrorKind::PreconditionFailed,
                        format!("Cannot find parent struct: {}", parent),
                    ))?;
                    let parent_field = if parent_field.field_type.is_nested() {
                        let parent_type = parent_field.field_type.as_ref();
                        match parent_type {
                            Type::List(nested) => nested.element_field.as_ref(), // fields are added to the element type
                            Type::Map(nested) => nested.value_field.as_ref(), // fields are added to the map value type
                            _ => parent_field,
                        }
                    } else {
                        parent_field
                    };
                    ensure_precondition!(
                        parent_field.field_type.is_struct(),
                        "Cannot add to non-struct column: {}: {}",
                        &parent,
                        parent_field.field_type
                    );
                    parent_id = parent_field.id;
                    let full_name = format!("{}.{}", parent, name);
                    let current_field = schema.field_by_name(&full_name);
                    ensure_precondition!(
                        !deletes.contains(&parent_id),
                        "Can not add a column that will be deleted: {}",
                        name
                    );
                    ensure_precondition!(
                        current_field.is_none() || deletes.contains(&current_field.unwrap().id),
                        "Cannot add column, name already exists: {}",
                        &name
                    );
                    full_name
                } else {
                    let current_field = schema.field_by_name(name);
                    ensure_precondition!(
                        current_field.is_none() || deletes.contains(&current_field.unwrap().id),
                        "Cannot add column, name already exists: {}",
                        &name
                    );
                    name.clone()
                };
                ensure_precondition!(
                    default_value.is_some() || is_optional,
                    "Incompatible change: cannot add required column without a default value: {}",
                    full_name
                );
                last_column_id += 1;
                let new_id = last_column_id;
                added_name_to_id.insert(full_name, new_id);

                if parent_id != TABLE_ROOT_ID {
                    id_to_parent.insert(new_id, parent_id);
                }
                let assigned_type = assign_fresh_ids(field_type.clone(), &mut last_column_id);
                let mut new_field = NestedField::new(new_id, name, assigned_type, !is_optional);
                if let Some(doc) = doc {
                    new_field.doc = doc.clone();
                }
                new_field.write_default = default_value.clone();
                new_field.initial_default = default_value.clone();
                updates.insert(new_id, new_field.into());
                parent_to_added_ids
                    .entry(parent_id)
                    .or_insert(vec![])
                    .push(new_id);
            }
            SchemaOperation::Delete(delete) => {
                let field = schema.field_by_name(&delete.name).ok_or_else(|| {
                    Error::new(
                        ErrorKind::PreconditionFailed,
                        format!("Cannot delete missing column: {}", delete.name),
                    )
                })?;
                ensure_precondition!(
                    !parent_to_added_ids.contains_key(&field.id),
                    "Cannot delete a column that has updates: {}",
                    delete.name
                );
                ensure_precondition!(
                    !updates.contains_key(&field.id),
                    "Cannot delete a column that has updates: {}",
                    delete.name
                );
                deletes.push(field.id);
            }
            SchemaOperation::Rename(rename) => {
                let (name, new_name) = (&rename.name, &rename.new_name);
                let field = schema.field_by_name(name).ok_or(Error::new(
                    ErrorKind::PreconditionFailed,
                    format!("Cannot rename missing column: {}", name),
                ))?;
                ensure_precondition!(
                    !deletes.contains(&field.id),
                    "Cannot rename a column that will be deleted: {}",
                    name
                );
                // merge with an update, if present
                let field_id = field.id;
                let update = updates.get(&field_id);
                let new_field = if let Some(update) = update {
                    Arc::unwrap_or_clone(update.clone()).with_name(new_name)
                } else {
                    Arc::unwrap_or_clone(field.clone()).with_name(new_name)
                };
                updates.insert(field_id, Arc::new(new_field));
                if identifier_field_ids.contains(&field_id) {
                    identifier_field_ids.remove(&field_id);
                    identifier_field_ids.insert(field_id);
                }
            }
            SchemaOperation::Update(update) => {
                let (name, new_type, new_doc, new_default_value) = (
                    &update.name,
                    &update.new_type,
                    &update.new_doc,
                    &update.new_default_value,
                );
                if let Some(new_type) = new_type {}
                if let Some(new_doc) = new_doc {}
                if let Some(new_default_value) = new_default_value {}
                todo!();
            }
            SchemaOperation::Move(r#move) => {}
        }
    }
    // validate identifier fields are not deleted
    // apply schema changes
    let mut visitor = ApplyChangesVisitor {
        deletes,
        updates,
        parent_to_added_ids,
        moves,
    };
    let struct_type = visit_schema(schema.as_ref(), &mut visitor)?
        .unwrap()
        .to_struct_type()
        .unwrap();
    // validate identifier requirements based on the latest schema
    Ok(Schema::builder()
        .with_fields(struct_type.fields().to_vec())
        .with_identifier_field_ids(identifier_field_ids)
        .build()?
        .into())
}

fn assign_fresh_ids(field_type: Type, next_id: &mut i32) -> Type {
    match field_type {
        Type::Primitive(_) => field_type,
        Type::Struct(s) => {
            let new_fields = s
                .fields()
                .iter()
                .map(|field| {
                    *next_id += 1;
                    let new_field_id = *next_id;
                    let new_type = assign_fresh_ids((*field.field_type).clone(), next_id);
                    Arc::new(NestedField::new(
                        new_field_id,
                        &field.name,
                        new_type,
                        field.required,
                    ))
                })
                .collect();
            Type::Struct(StructType::new(new_fields))
        }
        Type::List(list) => {
            *next_id += 1;
            let element_id = *next_id;
            let element_type = assign_fresh_ids((*list.element_field.field_type).clone(), next_id);
            Type::List(ListType::new(Arc::new(NestedField::new(
                element_id,
                &list.element_field.name,
                element_type,
                list.element_field.required,
            ))))
        }
        Type::Map(map) => {
            *next_id += 1;
            let key_id = *next_id;
            let key_type = assign_fresh_ids((*map.key_field.field_type).clone(), next_id);
            *next_id += 1;
            let value_id = *next_id;
            let value_type = assign_fresh_ids((*map.value_field.field_type).clone(), next_id);
            Type::Map(MapType::new(
                Arc::new(NestedField::new(
                    key_id,
                    &map.key_field.name,
                    key_type,
                    true,
                )),
                Arc::new(NestedField::new(
                    value_id,
                    &map.value_field.name,
                    value_type,
                    map.value_field.required,
                )),
            ))
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum MoveType {
    FIRST,
    BEFORE,
    AFTER,
}

#[derive(Clone, Debug)]
struct Move {
    field_id: i32,
    reference_field_id: i32,
    r#type: MoveType,
}

impl Move {
    fn first(field_id: i32) -> Self {
        Move::new(field_id, TABLE_ROOT_ID, MoveType::FIRST)
    }

    fn before(field_id: i32, reference_field_id: i32) -> Self {
        Move::new(field_id, reference_field_id, MoveType::BEFORE)
    }

    fn after(field_id: i32, reference_field_id: i32) -> Self {
        Move::new(field_id, reference_field_id, MoveType::AFTER)
    }

    fn new(field_id: i32, reference_field_id: i32, r#type: MoveType) -> Self {
        Move {
            field_id,
            reference_field_id,
            r#type,
        }
    }

    fn field_id(&self) -> i32 {
        self.field_id
    }

    fn reference_field_id(&self) -> i32 {
        self.reference_field_id
    }

    fn r#type(&self) -> MoveType {
        self.r#type
    }
}

struct ApplyChangesVisitor {
    deletes: Vec<i32>,
    updates: HashMap<i32, NestedFieldRef>,
    parent_to_added_ids: HashMap<i32, Vec<i32>>,
    moves: HashMap<i32, Vec<Move>>,
}

impl SchemaVisitor for ApplyChangesVisitor {
    type T = Option<Type>;

    fn schema(&mut self, schema: &Schema, value: Self::T) -> Result<Self::T> {
        let added_fields: Vec<NestedFieldRef> = self
            .parent_to_added_ids
            .get(&TABLE_ROOT_ID)
            .unwrap_or(&vec![])
            .iter()
            .map(|id| self.updates.get(id).unwrap().clone())
            .collect();
        let fields = add_and_move_fields(
            &value
                .clone()
                .unwrap()
                .to_struct_type()
                .unwrap()
                .fields()
                .to_vec(),
            &added_fields,
            self.moves.get(&TABLE_ROOT_ID).unwrap_or(&vec![]),
        );
        if !fields.is_empty() {
            return Ok(Some(Type::Struct(StructType::new(fields))));
        }
        Ok(value)
    }

    fn r#struct(&mut self, r#struct: &StructType, results: Vec<Self::T>) -> Result<Self::T> {
        let mut has_change = false;
        let mut new_fields: Vec<NestedFieldRef> = Vec::with_capacity(results.len());
        for i in 0..results.len() {
            let result_type = &results[i];
            if result_type.is_none() {
                has_change = true;
                continue;
            }
            let result_type = result_type.clone().unwrap();
            let field = &r#struct.fields()[i];
            let update = self.updates.get(&field.id);
            let updated = if let Some(update) = update {
                Arc::unwrap_or_clone(update.clone()).of_type(Box::new(result_type))
            } else {
                Arc::unwrap_or_clone(field.clone()).of_type(Box::new(result_type))
            };
            if field.as_ref() == &updated {
                new_fields.push(field.clone());
            } else {
                has_change = true;
                new_fields.push(updated.into());
            }
        }
        if has_change {
            return Ok(Some(Type::Struct(StructType::new(new_fields))));
        }
        Ok(Some(Type::Struct(r#struct.clone())))
    }

    fn field(&mut self, field: &NestedFieldRef, value: Self::T) -> Result<Self::T> {
        let field_id = field.id;
        // handle deletes
        if self.deletes.contains(&field_id) {
            return Ok(None);
        }
        // handle updates
        let update = self.updates.get(&field_id);
        if let Some(update) = update
            && update.field_type.as_ref() != field.field_type.as_ref()
        {
            return Ok(Some(*update.field_type.clone()));
        }
        // handle adds
        let new_fields: Vec<_> = self
            .parent_to_added_ids
            .get(&field_id)
            .unwrap_or(&vec![])
            .iter()
            .filter_map(|id| self.updates.get(id))
            .cloned()
            .collect();
        let columns_to_move = self.moves.get(&field_id).cloned().unwrap_or(vec![]);
        if !new_fields.is_empty() || !columns_to_move.is_empty() {
            let fields = add_and_move_fields(
                &value
                    .clone()
                    .unwrap()
                    .to_struct_type()
                    .unwrap()
                    .fields()
                    .to_vec(),
                &new_fields,
                &columns_to_move,
            );
            if !fields.is_empty() {
                return Ok(Some(Type::Struct(StructType::new(fields))));
            }
        }
        Ok(value)
    }

    fn list(&mut self, list: &ListType, element_result: Self::T) -> Result<Self::T> {
        let element_field = list.element_field.clone();
        let element_type = self
            .field(&element_field, element_result)?
            .ok_or(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot delete list element type from list: {:?}", list),
            ))?;
        let element_update = self.updates.get(&element_field.id);
        let is_element_optional = if let Some(element_update) = element_update {
            !element_update.required
        } else {
            !element_field.required
        };
        if !is_element_optional == element_field.required
            && &element_type == list.element_field.field_type.as_ref()
        {
            return Ok(Some(Type::List(list.clone())));
        }
        if is_element_optional {
            Ok(Some(Type::List(ListType::optional(
                list.element_field.id,
                element_type,
            ))))
        } else {
            Ok(Some(Type::List(ListType::required(
                list.element_field.id,
                element_type,
            ))))
        }
    }

    fn map(
        &mut self,
        map: &MapType,
        key_result: Self::T,
        value_result: Self::T,
    ) -> Result<Self::T> {
        let key_id = map.key_field.id;
        if self.deletes.contains(&key_id) {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot delete map keys: {:?}", map),
            ));
        } else if self.updates.contains_key(&key_id) {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot update map keys: {:?}", map),
            ));
        } else if self.parent_to_added_ids.contains_key(&key_id) {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot add fields to map keys: {:?}", map),
            ));
        } else if map.key_field.field_type.as_ref() != &key_result.unwrap() {
            return Err(Error::new(
                ErrorKind::PreconditionFailed,
                format!("Cannot alter map keys: {:?}", map),
            ));
        }
        let value_field = map.value_field.clone();
        let value_type = self.field(&value_field, value_result)?.ok_or(Error::new(
            ErrorKind::PreconditionFailed,
            format!("Cannot delete value type from map: {:?}", map),
        ))?;
        let value_update = self.updates.get(&value_field.id);
        let is_value_required = if let Some(update) = value_update {
            update.required
        } else {
            map.value_field.required
        };
        if is_value_required == map.value_field.required
            && map.value_field.field_type.as_ref() == &value_type
        {
            return Ok(Some(Type::Map(map.clone())));
        }
        if is_value_required {
            Ok(Some(Type::Map(MapType::required(
                map.key_field.id,
                *map.key_field.field_type.clone(),
                map.value_field.id,
                value_type,
            ))))
        } else {
            Ok(Some(Type::Map(MapType::optional(
                map.key_field.id,
                *map.key_field.field_type.clone(),
                map.value_field.id,
                value_type,
            ))))
        }
    }

    fn primitive(&mut self, p: &PrimitiveType) -> Result<Self::T> {
        Ok(Some(Type::Primitive(p.clone())))
    }
}

fn add_and_move_fields(
    fields: &Vec<NestedFieldRef>,
    adds: &Vec<NestedFieldRef>,
    moves: &Vec<Move>,
) -> Vec<NestedFieldRef> {
    if !adds.is_empty() {
        if !moves.is_empty() {
            return move_fields(&add_fields(fields, adds), moves);
        }
        return add_fields(fields, adds);
    } else if !moves.is_empty() {
        return move_fields(fields, moves);
    }
    vec![]
}

fn add_fields(fields: &Vec<NestedFieldRef>, adds: &Vec<NestedFieldRef>) -> Vec<NestedFieldRef> {
    let mut new_fields = fields.clone();
    new_fields.extend(adds.clone());
    new_fields
}

fn move_fields(fields: &[NestedFieldRef], moves: &Vec<Move>) -> Vec<NestedFieldRef> {
    let mut reordered = fields.to_vec();
    for r#move in moves {
        let idx = fields
            .iter()
            .position(|f| f.id == r#move.field_id())
            .unwrap();
        let to_move = reordered.remove(idx);
        match r#move.r#type() {
            MoveType::FIRST => {
                reordered.insert(0, to_move.clone());
            }
            MoveType::BEFORE => {
                let before_idx = fields
                    .iter()
                    .position(|f| f.id == r#move.reference_field_id())
                    .unwrap();
                reordered.insert(before_idx, to_move);
            }
            MoveType::AFTER => {
                let after_idx = fields
                    .iter()
                    .position(|f| f.id == r#move.reference_field_id())
                    .unwrap();
                reordered.insert(after_idx + 1, to_move);
            }
        }
    }
    reordered
}

mod tests {
    use std::sync::Arc;

    use crate::spec::update::{AddColumn, RenameColumn, SchemaOperation, schema_update};
    use crate::spec::{ListType, MapType, NestedField, PrimitiveType, Schema, StructType, Type};
    use crate::{Error, Result};

    fn make_schema() -> Schema {
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
                    Type::Map(MapType::new(
                        NestedField::map_key_element(18, Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::map_value_element(
                            19,
                            Type::Primitive(PrimitiveType::String),
                            true,
                        )
                        .into(),
                    )),
                )
                .with_doc("string map of properties")
                .into(),
            ])
            .build()
            .unwrap()
    }

    #[test]
    fn no_changes() {
        let base = make_schema();
        let expected = make_schema();
        let updated = schema_update(Arc::new(base), &vec![]).unwrap();
        assert_eq!(updated.as_ref(), &expected);
    }

    #[test]
    fn delete_fields() {
        let columns = vec![
            "id",
            "data",
            "preferences",
            "preferences.feature1",
            "preferences.feature2",
            "locations",
            "locations.lat",
            "locations.long",
            "points",
            "points.x",
            "points.y",
            "doubles",
            "properties",
        ];
    }

    #[test]
    fn delete_fields_case_sensitive_disabled() {
        let columns = vec![
            "Id",
            "Data",
            "Preferences",
            "Preferences.feature1",
            "Preferences.feature2",
            "Locations",
            "Locations.lat",
            "Locations.long",
            "Points",
            "Points.x",
            "Points.y",
            "Doubles",
            "Properties",
        ];
    }

    #[test]
    fn rename() {
        let renamed = schema_update(Arc::new(make_schema()), &vec![
            SchemaOperation::Rename(RenameColumn::new("data", "json")),
            SchemaOperation::Rename(RenameColumn::new("preferences", "options")),
            SchemaOperation::Rename(RenameColumn::new("preferences.feature2", "newfeature")),
            SchemaOperation::Rename(RenameColumn::new("locations.lat", "latitude")),
            SchemaOperation::Rename(RenameColumn::new("points.x", "X")),
            SchemaOperation::Rename(RenameColumn::new("points.y", "Y")),
        ]);
        let expected = Schema::builder()
            .with_fields(vec![
                NestedField::required(1, "id", Type::Primitive(PrimitiveType::Int)).into(),
                NestedField::optional(2, "json", Type::Primitive(PrimitiveType::String)).into(),
                NestedField::optional(
                    3,
                    "options",
                    Type::Struct(StructType::new(vec![
                        NestedField::required(
                            8,
                            "feature1",
                            Type::Primitive(PrimitiveType::Boolean),
                        )
                        .into(),
                        NestedField::optional(
                            9,
                            "newfeature",
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
                                    "latitude",
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
                        NestedField::list_element(
                            14,
                            Type::Struct(StructType::new(vec![
                                NestedField::required(
                                    15,
                                    "X",
                                    Type::Primitive(PrimitiveType::Long),
                                )
                                .into(),
                                NestedField::required(
                                    16,
                                    "Y",
                                    Type::Primitive(PrimitiveType::Long),
                                )
                                .into(),
                            ])),
                            false,
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
                    Type::Map(MapType::new(
                        NestedField::map_key_element(18, Type::Primitive(PrimitiveType::String))
                            .into(),
                        NestedField::map_value_element(
                            19,
                            Type::Primitive(PrimitiveType::String),
                            true,
                        )
                        .into(),
                    )),
                )
                .with_doc("string map of properties")
                .into(),
            ])
            .build()
            .unwrap();
        assert_eq!(renamed.unwrap().as_ref(), &expected);
    }

    #[test]
    fn rename_case_insensitive() {}

    #[test]
    fn add_fields() {
        let added = schema_update(Arc::new(make_schema()), &vec![
            SchemaOperation::Add(
                AddColumn::builder()
                    .name("topLevel")
                    .r#type(Type::Primitive(PrimitiveType::Decimal {
                        precision: 9,
                        scale: 2,
                    }))
                    .build(),
            ),
            SchemaOperation::Add(
                AddColumn::builder()
                    .parent("locations".to_string())
                    .name("alt")
                    .r#type(Type::Primitive(PrimitiveType::Float))
                    .build(),
            ),
            SchemaOperation::Add(
                AddColumn::builder()
                    .parent("points".to_string())
                    .name("z")
                    .r#type(Type::Primitive(PrimitiveType::Long))
                    .build(),
            ),
            SchemaOperation::Add(
                AddColumn::builder()
                    .parent("points".to_string())
                    .name("t.t")
                    .r#type(Type::Primitive(PrimitiveType::Long))
                    .build(),
            ),
        ])
        .unwrap();
    }
}
