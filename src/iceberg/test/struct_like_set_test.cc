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

#include "iceberg/util/struct_like_set.h"

#include <limits>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/schema_field.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

namespace iceberg {

class SimpleStructLike : public StructLike {
 public:
  explicit SimpleStructLike(std::vector<Scalar> fields) : fields_(std::move(fields)) {}

  Result<Scalar> GetField(size_t pos) const override {
    if (pos >= fields_.size()) {
      return NotFound("field position {} out of range [0, {})", pos, fields_.size());
    }
    return fields_[pos];
  }

  size_t num_fields() const override { return fields_.size(); }

  void SetField(size_t pos, Scalar value) { fields_[pos] = std::move(value); }

 private:
  std::vector<Scalar> fields_;
};

class SimpleArrayLike : public ArrayLike {
 public:
  explicit SimpleArrayLike(std::vector<Scalar> elements)
      : elements_(std::move(elements)) {}

  Result<Scalar> GetElement(size_t pos) const override {
    if (pos >= elements_.size()) {
      return NotFound("element position {} out of range [0, {})", pos, elements_.size());
    }
    return elements_[pos];
  }

  size_t size() const override { return elements_.size(); }

 private:
  std::vector<Scalar> elements_;
};

class SimpleMapLike : public MapLike {
 public:
  SimpleMapLike(std::vector<Scalar> keys, std::vector<Scalar> values)
      : keys_(std::move(keys)), values_(std::move(values)) {}

  Result<Scalar> GetKey(size_t pos) const override {
    if (pos >= keys_.size()) {
      return NotFound("key position {} out of range [0, {})", pos, keys_.size());
    }
    return keys_[pos];
  }

  Result<Scalar> GetValue(size_t pos) const override {
    if (pos >= values_.size()) {
      return NotFound("value position {} out of range [0, {})", pos, values_.size());
    }
    return values_[pos];
  }

  size_t size() const override { return keys_.size(); }

 private:
  std::vector<Scalar> keys_;
  std::vector<Scalar> values_;
};

class FailingStructLike : public StructLike {
 public:
  explicit FailingStructLike(size_t num_fields) : num_fields_(num_fields) {}

  Result<Scalar> GetField(size_t pos) const override {
    return NotFound("boom at field {}", pos);
  }

  size_t num_fields() const override { return num_fields_; }

 private:
  size_t num_fields_;
};

StructType MakeStructType(
    std::vector<std::pair<std::string, std::shared_ptr<Type>>> fields) {
  std::vector<SchemaField> schema_fields;
  schema_fields.reserve(fields.size());
  int32_t id = 1;
  for (auto& [name, type] : fields) {
    schema_fields.push_back(SchemaField::MakeOptional(id++, name, std::move(type)));
  }
  return StructType(std::move(schema_fields));
}

TEST(StructLikeSetTest, EmptySet) {
  auto type = MakeStructType({{"id", int32()}});
  StructLikeSet set(type);

  EXPECT_TRUE(set.IsEmpty());
  EXPECT_EQ(set.Size(), 0);

  SimpleStructLike row({Scalar{int32_t{1}}});
  EXPECT_THAT(set.Contains(row), HasValue(::testing::Eq(false)));
}

TEST(StructLikeSetTest, InsertAndContains) {
  auto type = MakeStructType({{"id", int32()}, {"name", string()}});
  StructLikeSet set(type);

  std::string name1 = "alice";
  std::string name2 = "bob";

  SimpleStructLike row1({Scalar{int32_t{1}}, Scalar{std::string_view(name1)}});
  SimpleStructLike row2({Scalar{int32_t{2}}, Scalar{std::string_view(name2)}});

  ASSERT_THAT(set.Insert(row1), IsOk());
  ASSERT_THAT(set.Insert(row2), IsOk());

  EXPECT_EQ(set.Size(), 2);
  EXPECT_FALSE(set.IsEmpty());
  EXPECT_THAT(set.Contains(row1), HasValue(::testing::Eq(true)));
  EXPECT_THAT(set.Contains(row2), HasValue(::testing::Eq(true)));

  // Row not in the set
  std::string name3 = "charlie";
  SimpleStructLike row3({Scalar{int32_t{3}}, Scalar{std::string_view(name3)}});
  EXPECT_THAT(set.Contains(row3), HasValue(::testing::Eq(false)));
}

TEST(StructLikeSetTest, DuplicateInsert) {
  auto type = MakeStructType({{"id", int32()}});
  StructLikeSet set(type);

  SimpleStructLike row({Scalar{int32_t{42}}});
  ASSERT_THAT(set.Insert(row), IsOk());
  EXPECT_EQ(set.Size(), 1);

  // Duplicate insertion should not increase size
  ASSERT_THAT(set.Insert(row), IsOk());
  EXPECT_EQ(set.Size(), 1);
}

TEST(StructLikeSetTest, FieldsWithNulls) {
  auto type = MakeStructType({{"id", int32()}, {"data", int64()}});
  StructLikeSet set(type);

  // Row with null in second field
  SimpleStructLike row1({Scalar{int32_t{1}}, Scalar{std::monostate{}}});
  SimpleStructLike row2({Scalar{int32_t{2}}, Scalar{std::monostate{}}});

  ASSERT_THAT(set.Insert(row1), IsOk());
  ASSERT_THAT(set.Insert(row2), IsOk());

  EXPECT_EQ(set.Size(), 2);
  EXPECT_THAT(set.Contains(row1), HasValue(::testing::Eq(true)));
  EXPECT_THAT(set.Contains(row2), HasValue(::testing::Eq(true)));

  // Same key as row1 — should match
  SimpleStructLike row1_copy({Scalar{int32_t{1}}, Scalar{std::monostate{}}});
  EXPECT_THAT(set.Contains(row1_copy), HasValue(::testing::Eq(true)));
}

TEST(StructLikeSetTest, StringFieldOwnership) {
  auto type = MakeStructType({{"name", std::make_shared<StringType>()}});
  StructLikeSet set(type);

  // Insert with a temporary string that will be destroyed
  {
    std::string temp = "temporary_string_data";
    SimpleStructLike row({Scalar{std::string_view(temp)}});
    ASSERT_THAT(set.Insert(row), IsOk());
  }
  // temp is destroyed here — arena should hold the copy

  EXPECT_EQ(set.Size(), 1);

  // Look up with a new string that has the same content
  std::string lookup = "temporary_string_data";
  SimpleStructLike lookup_row({Scalar{std::string_view(lookup)}});
  EXPECT_THAT(set.Contains(lookup_row), HasValue(::testing::Eq(true)));
}

TEST(StructLikeSetTest, MultipleTypes) {
  auto type = MakeStructType({{"b", boolean()},
                              {"i", int32()},
                              {"l", int64()},
                              {"f", float32()},
                              {"d", float64()},
                              {"s", string()},
                              {"dt", date()}});
  StructLikeSet set(type);

  std::string str = "hello";
  SimpleStructLike row({Scalar{true}, Scalar{int32_t{1}}, Scalar{int64_t{2}},
                        Scalar{1.0f}, Scalar{2.0}, Scalar{std::string_view(str)},
                        Scalar{int32_t{19000}}});
  ASSERT_THAT(set.Insert(row), IsOk());
  EXPECT_THAT(set.Contains(row), HasValue(::testing::Eq(true)));

  // Different values → not found
  SimpleStructLike row2({Scalar{false}, Scalar{int32_t{1}}, Scalar{int64_t{2}},
                         Scalar{1.0f}, Scalar{2.0}, Scalar{std::string_view(str)},
                         Scalar{int32_t{19000}}});
  EXPECT_THAT(set.Contains(row2), HasValue(::testing::Eq(false)));
}

TEST(StructLikeSetTest, NestedStruct) {
  auto inner_type = struct_({SchemaField::MakeOptional(10, "x", int32()),
                             SchemaField::MakeOptional(11, "y", string())});
  auto outer_type = MakeStructType({{"id", int32()}, {"nested", inner_type}});
  StructLikeSet set(outer_type);

  // Create nested StructLike
  std::string inner_str = "nested_value";
  auto inner = std::make_shared<SimpleStructLike>(
      std::vector<Scalar>{Scalar{int32_t{10}}, Scalar{std::string_view(inner_str)}});

  SimpleStructLike row({Scalar{int32_t{1}}, Scalar{std::shared_ptr<StructLike>(inner)}});
  ASSERT_THAT(set.Insert(row), IsOk());
  EXPECT_EQ(set.Size(), 1);

  // Look up with same nested content (different object)
  std::string inner_str2 = "nested_value";
  auto inner2 = std::make_shared<SimpleStructLike>(
      std::vector<Scalar>{Scalar{int32_t{10}}, Scalar{std::string_view(inner_str2)}});
  SimpleStructLike lookup(
      {Scalar{int32_t{1}}, Scalar{std::shared_ptr<StructLike>(inner2)}});
  EXPECT_THAT(set.Contains(lookup), HasValue(::testing::Eq(true)));

  // Different nested content → not found
  std::string inner_str3 = "different";
  auto inner3 = std::make_shared<SimpleStructLike>(
      std::vector<Scalar>{Scalar{int32_t{10}}, Scalar{std::string_view(inner_str3)}});
  SimpleStructLike different(
      {Scalar{int32_t{1}}, Scalar{std::shared_ptr<StructLike>(inner3)}});
  EXPECT_THAT(set.Contains(different), HasValue(::testing::Eq(false)));
}

TEST(StructLikeSetTest, NestedStructOwnership) {
  auto inner_type = struct_({SchemaField::MakeOptional(10, "s", string())});
  auto outer_type = MakeStructType({{"nested", inner_type}});
  StructLikeSet set(outer_type);

  // Insert with temporary inner data
  {
    std::string temp = "will_be_destroyed";
    auto inner = std::make_shared<SimpleStructLike>(
        std::vector<Scalar>{Scalar{std::string_view(temp)}});
    SimpleStructLike row({Scalar{std::shared_ptr<StructLike>(inner)}});
    ASSERT_THAT(set.Insert(row), IsOk());
  }
  // temp and inner are destroyed here. Arena should hold copies.

  EXPECT_EQ(set.Size(), 1);

  // Look up with new identical content
  std::string lookup_str = "will_be_destroyed";
  auto inner2 = std::make_shared<SimpleStructLike>(
      std::vector<Scalar>{Scalar{std::string_view(lookup_str)}});
  SimpleStructLike lookup({Scalar{std::shared_ptr<StructLike>(inner2)}});
  EXPECT_THAT(set.Contains(lookup), HasValue(::testing::Eq(true)));
}

TEST(StructLikeSetTest, AllNullRow) {
  auto type = MakeStructType({{"a", int32()}, {"b", string()}});
  StructLikeSet set(type);

  SimpleStructLike null_row({Scalar{std::monostate{}}, Scalar{std::monostate{}}});
  ASSERT_THAT(set.Insert(null_row), IsOk());
  EXPECT_EQ(set.Size(), 1);
  EXPECT_THAT(set.Contains(null_row), HasValue(::testing::Eq(true)));

  // Duplicate null row
  SimpleStructLike null_row2({Scalar{std::monostate{}}, Scalar{std::monostate{}}});
  ASSERT_THAT(set.Insert(null_row2), IsOk());
  EXPECT_EQ(set.Size(), 1);
}

TEST(StructLikeSetTest, ContainsPropagatesFieldAccessError) {
  auto type = MakeStructType({{"id", int32()}});
  StructLikeSet set(type);

  FailingStructLike row(1);
  EXPECT_THAT(set.Contains(row), IsError(ErrorKind::kNotFound));
}

TEST(StructLikeSetTest, InsertPropagatesFieldAccessError) {
  auto type = MakeStructType({{"id", int32()}});
  StructLikeSet set(type);

  FailingStructLike row(1);
  EXPECT_THAT(set.Insert(row), IsError(ErrorKind::kNotFound));
}

TEST(StructLikeSetTest, InsertRejectsFieldCountMismatch) {
  auto type = MakeStructType({{"id", int32()}, {"name", string()}});
  StructLikeSet set(type);

  SimpleStructLike row({Scalar{int32_t{1}}});
  EXPECT_THAT(set.Insert(row), IsError(ErrorKind::kInvalidArgument));
}

TEST(StructLikeSetTest, ContainsRejectsFieldTypeMismatch) {
  auto type = MakeStructType({{"id", int32()}});
  StructLikeSet set(type);

  SimpleStructLike row({Scalar{std::string_view("not_an_int")}});
  EXPECT_THAT(set.Contains(row), IsError(ErrorKind::kInvalidArgument));
}

TEST(StructLikeSetTest, FloatAndDoubleFollowJavaEqualitySemantics) {
  auto type = MakeStructType({{"f", float32()}, {"d", float64()}});
  StructLikeSet set(type);

  float float_nan = std::numeric_limits<float>::quiet_NaN();
  double double_nan = std::numeric_limits<double>::quiet_NaN();
  SimpleStructLike nan_row({Scalar{float_nan}, Scalar{double_nan}});
  ASSERT_THAT(set.Insert(nan_row), IsOk());

  float another_float_nan = std::numeric_limits<float>::signaling_NaN();
  double another_double_nan = std::numeric_limits<double>::signaling_NaN();
  SimpleStructLike lookup_nan({Scalar{another_float_nan}, Scalar{another_double_nan}});
  EXPECT_THAT(set.Contains(lookup_nan), HasValue(::testing::Eq(true)));

  SimpleStructLike neg_zero({Scalar{-0.0f}, Scalar{-0.0}});
  SimpleStructLike pos_zero({Scalar{0.0f}, Scalar{0.0}});
  ASSERT_THAT(set.Insert(neg_zero), IsOk());
  EXPECT_THAT(set.Contains(pos_zero), HasValue(::testing::Eq(false)));
  ASSERT_THAT(set.Insert(pos_zero), IsOk());
  EXPECT_EQ(set.Size(), 3);
}

TEST(StructLikeSetTest, NestedMapIsHashedAndComparedRecursively) {
  auto map_type =
      std::make_shared<MapType>(SchemaField::MakeRequired(10, "key", string()),
                                SchemaField::MakeOptional(11, "value", int32()));
  auto type = MakeStructType({{"m", map_type}});
  StructLikeSet set(type);

  std::string key1 = "a";
  auto map1 =
      std::make_shared<SimpleMapLike>(std::vector<Scalar>{Scalar{std::string_view(key1)}},
                                      std::vector<Scalar>{Scalar{int32_t{7}}});
  SimpleStructLike row({Scalar{std::shared_ptr<MapLike>(map1)}});
  ASSERT_THAT(set.Insert(row), IsOk());

  std::string key2 = "a";
  auto map2 =
      std::make_shared<SimpleMapLike>(std::vector<Scalar>{Scalar{std::string_view(key2)}},
                                      std::vector<Scalar>{Scalar{int32_t{7}}});
  SimpleStructLike same({Scalar{std::shared_ptr<MapLike>(map2)}});
  EXPECT_THAT(set.Contains(same), HasValue(::testing::Eq(true)));

  std::string key3 = "b";
  auto map3 =
      std::make_shared<SimpleMapLike>(std::vector<Scalar>{Scalar{std::string_view(key3)}},
                                      std::vector<Scalar>{Scalar{int32_t{7}}});
  SimpleStructLike different({Scalar{std::shared_ptr<MapLike>(map3)}});
  EXPECT_THAT(set.Contains(different), HasValue(::testing::Eq(false)));
}

}  // namespace iceberg
