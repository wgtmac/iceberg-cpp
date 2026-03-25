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

#include "iceberg/deletes/roaring_position_bitmap.h"

#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <random>
#include <set>
#include <string>
#include <vector>

#include <gtest/gtest.h>

#include "iceberg/test/matchers.h"
#include "iceberg/test/test_config.h"

namespace iceberg {

namespace {

constexpr int64_t kBitmapSize = 0xFFFFFFFFL;
constexpr int64_t kBitmapOffset = kBitmapSize + 1L;
constexpr int64_t kContainerSize = 0xFFFF;  // Character.MAX_VALUE
constexpr int64_t kContainerOffset = kContainerSize + 1L;

// Helper to construct a position from bitmap index, container index, and value
int64_t Position(int32_t bitmap_index, int32_t container_index, int64_t value) {
  return bitmap_index * kBitmapOffset + container_index * kContainerOffset + value;
}

std::string ReadTestResource(const std::string& filename) {
  std::filesystem::path path = std::filesystem::path(ICEBERG_TEST_RESOURCES) / filename;
  std::ifstream file(path, std::ios::binary);
  EXPECT_TRUE(file.good()) << "Cannot open: " << path;
  return {std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
}

RoaringPositionBitmap RoundTripSerialize(const RoaringPositionBitmap& bitmap) {
  auto serialized = bitmap.Serialize();
  EXPECT_THAT(serialized, IsOk());
  auto deserialized = RoaringPositionBitmap::Deserialize(serialized.value());
  EXPECT_THAT(deserialized, IsOk());
  return std::move(deserialized.value());
}

void AssertEqualContent(const RoaringPositionBitmap& bitmap,
                        const std::set<int64_t>& positions) {
  ASSERT_EQ(bitmap.Cardinality(), positions.size());
  for (int64_t pos : positions) {
    ASSERT_THAT(bitmap.Contains(pos), HasValue(::testing::Eq(true)))
        << "Position not found: " << pos;
  }
  bitmap.ForEach([&](int64_t pos) {
    ASSERT_TRUE(positions.contains(pos)) << "Unexpected position: " << pos;
  });
}

void AssertEqual(RoaringPositionBitmap& bitmap, const std::set<int64_t>& positions) {
  AssertEqualContent(bitmap, positions);

  auto copy1 = RoundTripSerialize(bitmap);
  AssertEqualContent(copy1, positions);

  bitmap.Optimize();
  auto copy2 = RoundTripSerialize(bitmap);
  AssertEqualContent(copy2, positions);
}

RoaringPositionBitmap BuildBitmap(const std::set<int64_t>& positions) {
  RoaringPositionBitmap bitmap;
  for (int64_t pos : positions) {
    EXPECT_THAT(bitmap.Add(pos), IsOk());
  }
  return bitmap;
}

struct AddRangeParams {
  const char* name;
  int64_t start;
  int64_t end;
  std::vector<int64_t> absent_positions;
};

class RoaringPositionBitmapAddRangeTest
    : public ::testing::TestWithParam<AddRangeParams> {};

TEST_P(RoaringPositionBitmapAddRangeTest, AddsExpectedPositions) {
  const auto& param = GetParam();
  RoaringPositionBitmap bitmap;
  ASSERT_THAT(bitmap.AddRange(param.start, param.end), IsOk());

  std::set<int64_t> expected_positions;
  for (int64_t pos = param.start; pos < param.end; ++pos) {
    expected_positions.insert(pos);
  }
  AssertEqualContent(bitmap, expected_positions);
  for (int64_t pos : param.absent_positions) {
    ASSERT_FALSE(bitmap.Contains(pos).value());
  }
}

INSTANTIATE_TEST_SUITE_P(
    AddRangeScenarios, RoaringPositionBitmapAddRangeTest,
    ::testing::Values(AddRangeParams{.name = "single_key",
                                     .start = 10,
                                     .end = 20,
                                     .absent_positions = {9, 20}},
                      AddRangeParams{
                          .name = "across_keys",
                          .start = (int64_t{1} << 32) - 5,
                          .end = (int64_t{1} << 32) + 5,
                          .absent_positions = {0, (int64_t{1} << 32) + 5},
                      }),
    [](const ::testing::TestParamInfo<AddRangeParams>& info) { return info.param.name; });

struct OrParams {
  const char* name;
  std::set<int64_t> lhs_input;
  std::set<int64_t> rhs_input;
  std::set<int64_t> lhs_expected;
  std::set<int64_t> rhs_expected;
};

class RoaringPositionBitmapOrTest : public ::testing::TestWithParam<OrParams> {};

TEST_P(RoaringPositionBitmapOrTest, ProducesExpectedUnionAndKeepsRhsUnchanged) {
  const auto& param = GetParam();
  auto lhs = BuildBitmap(param.lhs_input);
  auto rhs = BuildBitmap(param.rhs_input);

  lhs.Or(rhs);

  AssertEqualContent(lhs, param.lhs_expected);
  AssertEqualContent(rhs, param.rhs_expected);
}

INSTANTIATE_TEST_SUITE_P(
    OrScenarios, RoaringPositionBitmapOrTest,
    ::testing::Values(
        OrParams{
            .name = "disjoint",
            .lhs_input = {10L, 20L},
            .rhs_input = {30L, 40L, int64_t{2} << 32},
            .lhs_expected = {10L, 20L, 30L, 40L, int64_t{2} << 32},
            .rhs_expected = {30L, 40L, int64_t{2} << 32},
        },
        OrParams{
            .name = "rhs_empty",
            .lhs_input = {10L, 20L},
            .rhs_input = {},
            .lhs_expected = {10L, 20L},
            .rhs_expected = {},
        },
        OrParams{
            .name = "overlapping",
            .lhs_input = {10L, 20L, 30L},
            .rhs_input = {20L, 40L},
            .lhs_expected = {10L, 20L, 30L, 40L},
            .rhs_expected = {20L, 40L},
        },
        OrParams{
            .name = "sparse_multi_key",
            .lhs_input = {100L, (int64_t{1} << 32) | 200L},
            .rhs_input = {(int64_t{2} << 32) | 300L, (int64_t{3} << 32) | 400L},
            .lhs_expected = {100L, (int64_t{1} << 32) | 200L, (int64_t{2} << 32) | 300L,
                             (int64_t{3} << 32) | 400L},
            .rhs_expected = {(int64_t{2} << 32) | 300L, (int64_t{3} << 32) | 400L},
        }),
    [](const ::testing::TestParamInfo<OrParams>& info) { return info.param.name; });

enum class InteropBitmapShape {
  kEmpty,
  kOnly32BitPositions,
  kSpreadAcrossKeys,
};

struct InteropCase {
  const char* file_name;
  InteropBitmapShape expected_shape;
};

void AssertInteropBitmapShape(const RoaringPositionBitmap& bitmap,
                              InteropBitmapShape expected_shape) {
  bool saw_pos_lt_32_bit = false;
  bool saw_pos_ge_32_bit = false;

  bitmap.ForEach([&](int64_t pos) {
    if (pos < (int64_t{1} << 32)) {
      saw_pos_lt_32_bit = true;
    } else {
      saw_pos_ge_32_bit = true;
    }
  });

  switch (expected_shape) {
    case InteropBitmapShape::kEmpty:
      ASSERT_TRUE(bitmap.IsEmpty());
      ASSERT_EQ(bitmap.Cardinality(), 0u);
      break;
    case InteropBitmapShape::kOnly32BitPositions:
      ASSERT_GT(bitmap.Cardinality(), 0u);
      ASSERT_TRUE(saw_pos_lt_32_bit);
      ASSERT_FALSE(saw_pos_ge_32_bit);
      break;
    case InteropBitmapShape::kSpreadAcrossKeys:
      ASSERT_GT(bitmap.Cardinality(), 0u);
      ASSERT_TRUE(saw_pos_lt_32_bit);
      ASSERT_TRUE(saw_pos_ge_32_bit);
      break;
  }
}

}  // namespace

TEST(RoaringPositionBitmapTest, TestAdd) {
  RoaringPositionBitmap bitmap;

  ASSERT_THAT(bitmap.Add(10L), IsOk());
  ASSERT_TRUE(bitmap.Contains(10L).value());

  ASSERT_THAT(bitmap.Add(0L), IsOk());
  ASSERT_TRUE(bitmap.Contains(0L).value());

  ASSERT_THAT(bitmap.Add(10L), IsOk());  // duplicate
  ASSERT_TRUE(bitmap.Contains(10L).value());
}

TEST(RoaringPositionBitmapTest, TestAddPositionsRequiringMultipleBitmaps) {
  RoaringPositionBitmap bitmap;

  int64_t pos1 = 10L;
  int64_t pos2 = (int64_t{1} << 32) | 20L;
  int64_t pos3 = (int64_t{2} << 32) | 30L;
  int64_t pos4 = (int64_t{100} << 32) | 40L;

  ASSERT_THAT(bitmap.Add(pos1), IsOk());
  ASSERT_THAT(bitmap.Add(pos2), IsOk());
  ASSERT_THAT(bitmap.Add(pos3), IsOk());
  ASSERT_THAT(bitmap.Add(pos4), IsOk());

  AssertEqualContent(bitmap, {pos1, pos2, pos3, pos4});
  ASSERT_EQ(bitmap.SerializedSizeInBytes(), 1260);
}

TEST(RoaringPositionBitmapTest, TestAddEmptyRange) {
  RoaringPositionBitmap bitmap;
  ASSERT_THAT(bitmap.AddRange(10, 10), IsOk());
  ASSERT_TRUE(bitmap.IsEmpty());
}

TEST(RoaringPositionBitmapTest, TestCardinality) {
  RoaringPositionBitmap bitmap;

  ASSERT_EQ(bitmap.Cardinality(), 0);

  ASSERT_THAT(bitmap.Add(10L), IsOk());
  ASSERT_THAT(bitmap.Add(20L), IsOk());
  ASSERT_THAT(bitmap.Add(30L), IsOk());
  ASSERT_EQ(bitmap.Cardinality(), 3);

  ASSERT_THAT(bitmap.Add(10L), IsOk());  // already exists
  ASSERT_EQ(bitmap.Cardinality(), 3);
}

TEST(RoaringPositionBitmapTest, TestCardinalitySparseBitmaps) {
  RoaringPositionBitmap bitmap;

  ASSERT_THAT(bitmap.Add(100L), IsOk());
  ASSERT_THAT(bitmap.Add(101L), IsOk());
  ASSERT_THAT(bitmap.Add(105L), IsOk());
  ASSERT_THAT(bitmap.Add((int64_t{1} << 32) | 200L), IsOk());
  ASSERT_THAT(bitmap.Add((int64_t{100} << 32) | 300L), IsOk());

  ASSERT_EQ(bitmap.Cardinality(), 5);
}

TEST(RoaringPositionBitmapTest, TestSerializeDeserializeRoundTrip) {
  RoaringPositionBitmap bitmap;
  ASSERT_THAT(bitmap.Add(10L), IsOk());
  ASSERT_THAT(bitmap.Add(20L), IsOk());
  ASSERT_THAT(bitmap.Add((int64_t{1} << 32) | 30L), IsOk());

  auto copy = RoundTripSerialize(bitmap);
  AssertEqualContent(copy, {10L, 20L, (int64_t{1} << 32) | 30L});
}

TEST(RoaringPositionBitmapTest, TestCopyConstructor) {
  RoaringPositionBitmap bitmap;
  ASSERT_THAT(bitmap.Add(10L), IsOk());
  ASSERT_THAT(bitmap.Add((int64_t{2} << 32) | 30L), IsOk());

  RoaringPositionBitmap copy(bitmap);

  AssertEqualContent(copy, {10L, (int64_t{2} << 32) | 30L});

  // Copy is independent from source.
  ASSERT_THAT(copy.Add(99L), IsOk());
  ASSERT_THAT(bitmap.Contains(99L), HasValue(::testing::Eq(false)));
}

TEST(RoaringPositionBitmapTest, TestCopyAssignment) {
  RoaringPositionBitmap bitmap;
  ASSERT_THAT(bitmap.Add(10L), IsOk());
  ASSERT_THAT(bitmap.Add((int64_t{3} << 32) | 40L), IsOk());

  RoaringPositionBitmap assigned;
  ASSERT_THAT(assigned.Add(1L), IsOk());

  assigned = bitmap;
  AssertEqualContent(assigned, {10L, (int64_t{3} << 32) | 40L});

  // Assignment result is independent from source.
  ASSERT_THAT(bitmap.Add(200L), IsOk());
  ASSERT_THAT(assigned.Contains(200L), HasValue(::testing::Eq(false)));
}

TEST(RoaringPositionBitmapTest, TestSerializeDeserializeEmpty) {
  RoaringPositionBitmap bitmap;
  auto copy = RoundTripSerialize(bitmap);
  ASSERT_TRUE(copy.IsEmpty());
  ASSERT_EQ(copy.Cardinality(), 0);
}

TEST(RoaringPositionBitmapTest, TestSerializeDeserializeAllContainerBitmap) {
  RoaringPositionBitmap bitmap;

  // bitmap 0, container 0 (array - few elements)
  ASSERT_THAT(bitmap.Add(Position(0, 0, 5)), IsOk());
  ASSERT_THAT(bitmap.Add(Position(0, 0, 7)), IsOk());

  // bitmap 0, container 1 (array that can be compressed)
  ASSERT_THAT(bitmap.AddRange(Position(0, 1, 1), Position(0, 1, 1000)), IsOk());

  // bitmap 0, container 2 (bitset - nearly full container)
  ASSERT_THAT(bitmap.AddRange(Position(0, 2, 1), Position(0, 2, kContainerOffset - 1)),
              IsOk());

  // bitmap 1, container 0 (array)
  ASSERT_THAT(bitmap.Add(Position(1, 0, 10)), IsOk());
  ASSERT_THAT(bitmap.Add(Position(1, 0, 20)), IsOk());

  // bitmap 1, container 1 (array that can be compressed)
  ASSERT_THAT(bitmap.AddRange(Position(1, 1, 10), Position(1, 1, 500)), IsOk());

  // bitmap 1, container 2 (bitset)
  ASSERT_THAT(bitmap.AddRange(Position(1, 2, 1), Position(1, 2, kContainerOffset - 1)),
              IsOk());

  ASSERT_TRUE(bitmap.Optimize());

  auto copy = RoundTripSerialize(bitmap);
  std::set<int64_t> expected_positions;
  bitmap.ForEach([&](int64_t pos) { expected_positions.insert(pos); });

  AssertEqualContent(copy, expected_positions);
}

TEST(RoaringPositionBitmapTest, TestForEach) {
  RoaringPositionBitmap bitmap;
  ASSERT_THAT(bitmap.Add(30L), IsOk());
  ASSERT_THAT(bitmap.Add(10L), IsOk());
  ASSERT_THAT(bitmap.Add(20L), IsOk());
  ASSERT_THAT(bitmap.Add((int64_t{1} << 32) | 5L), IsOk());

  std::vector<int64_t> positions;
  bitmap.ForEach([&](int64_t pos) { positions.push_back(pos); });

  ASSERT_EQ(positions.size(), 4u);
  ASSERT_EQ(positions[0], 10L);
  ASSERT_EQ(positions[1], 20L);
  ASSERT_EQ(positions[2], 30L);
  ASSERT_EQ(positions[3], (int64_t{1} << 32) | 5L);
}

TEST(RoaringPositionBitmapTest, TestIsEmpty) {
  RoaringPositionBitmap bitmap;
  ASSERT_TRUE(bitmap.IsEmpty());

  ASSERT_THAT(bitmap.Add(10L), IsOk());
  ASSERT_FALSE(bitmap.IsEmpty());
}

TEST(RoaringPositionBitmapTest, TestOptimize) {
  RoaringPositionBitmap bitmap;
  ASSERT_THAT(bitmap.AddRange(0, 10000), IsOk());
  size_t size_before = bitmap.SerializedSizeInBytes();

  bool changed = bitmap.Optimize();
  ASSERT_TRUE(changed);

  // RLE optimization should reduce size for this dense range
  size_t size_after = bitmap.SerializedSizeInBytes();
  ASSERT_GT(size_before, size_after);

  // Content should be unchanged after RLE optimization
  std::set<int64_t> expected_positions;
  for (int64_t i = 0; i < 10000; ++i) {
    expected_positions.insert(i);
  }
  AssertEqualContent(bitmap, expected_positions);

  // Round-trip should preserve content after RLE
  auto copy = RoundTripSerialize(bitmap);
  AssertEqualContent(copy, expected_positions);
}

TEST(RoaringPositionBitmapTest, TestUnsupportedPositions) {
  RoaringPositionBitmap bitmap;

  // Negative position
  ASSERT_THAT(bitmap.Add(-1L), IsError(ErrorKind::kInvalidArgument));

  // Contains with negative position
  ASSERT_THAT(bitmap.Contains(-1L), IsError(ErrorKind::kInvalidArgument));

  // Position exceeding MAX_POSITION
  ASSERT_THAT(bitmap.Add(RoaringPositionBitmap::kMaxPosition + 1L),
              IsError(ErrorKind::kInvalidArgument));

  // Contains with position exceeding MAX_POSITION
  ASSERT_THAT(bitmap.Contains(RoaringPositionBitmap::kMaxPosition + 1L),
              IsError(ErrorKind::kInvalidArgument));
}

TEST(RoaringPositionBitmapTest, TestRandomSparseBitmap) {
  std::mt19937_64 rng(42);
  RoaringPositionBitmap bitmap;
  std::set<int64_t> positions;

  std::uniform_int_distribution<int64_t> dist(0, int64_t{5} << 32);

  for (int i = 0; i < 100000; ++i) {
    int64_t pos = dist(rng);
    positions.insert(pos);
    ASSERT_THAT(bitmap.Add(pos), IsOk());
  }

  AssertEqual(bitmap, positions);

  // Random lookups
  std::mt19937_64 rng2(123);
  std::uniform_int_distribution<int64_t> lookup_dist(0,
                                                     RoaringPositionBitmap::kMaxPosition);
  for (int i = 0; i < 20000; ++i) {
    int64_t pos = lookup_dist(rng2);
    auto result = bitmap.Contains(pos);
    ASSERT_THAT(result, IsOk());
    ASSERT_EQ(result.value(), positions.contains(pos));
  }
}

TEST(RoaringPositionBitmapTest, TestRandomDenseBitmap) {
  RoaringPositionBitmap bitmap;
  std::set<int64_t> positions;

  // Create dense ranges across multiple bitmap keys
  for (int64_t offset : {int64_t{0}, int64_t{2} << 32, int64_t{5} << 32}) {
    for (int64_t i = 0; i < 10000; ++i) {
      ASSERT_THAT(bitmap.Add(offset + i), IsOk());
      positions.insert(offset + i);
    }
  }

  AssertEqual(bitmap, positions);
}

TEST(RoaringPositionBitmapTest, TestRandomMixedBitmap) {
  std::mt19937_64 rng(42);
  RoaringPositionBitmap bitmap;
  std::set<int64_t> positions;

  // Sparse positions in [3<<32, 5<<32)
  std::uniform_int_distribution<int64_t> dist1(int64_t{3} << 32, int64_t{5} << 32);
  for (int i = 0; i < 50000; ++i) {
    int64_t pos = dist1(rng);
    positions.insert(pos);
    ASSERT_THAT(bitmap.Add(pos), IsOk());
  }

  // Dense range in [0, 10000)
  for (int64_t i = 0; i < 10000; ++i) {
    ASSERT_THAT(bitmap.Add(i), IsOk());
    positions.insert(i);
  }

  // More sparse positions in [0, 1<<32)
  std::uniform_int_distribution<int64_t> dist2(0, int64_t{1} << 32);
  for (int i = 0; i < 5000; ++i) {
    int64_t pos = dist2(rng);
    positions.insert(pos);
    ASSERT_THAT(bitmap.Add(pos), IsOk());
  }

  AssertEqual(bitmap, positions);
}

TEST(RoaringPositionBitmapTest, TestDeserializeInvalidData) {
  // Buffer too small
  auto result = RoaringPositionBitmap::Deserialize("");
  ASSERT_THAT(result, IsError(ErrorKind::kInvalidArgument));

  // Invalid bitmap count (very large)
  std::string buf(8, '\xFF');
  result = RoaringPositionBitmap::Deserialize(buf);
  ASSERT_THAT(result, IsError(ErrorKind::kInvalidArgument));
}

TEST(RoaringPositionBitmapInteropTest, TestDeserializeSupportedRoaringExamples) {
  // These .bin fixtures are copied from the Apache Iceberg Java repository's
  // roaring position bitmap interoperability test resources.
  static const std::vector<InteropCase> kCases = {
      {.file_name = "64map32bitvals.bin",
       .expected_shape = InteropBitmapShape::kOnly32BitPositions},
      {.file_name = "64mapempty.bin", .expected_shape = InteropBitmapShape::kEmpty},
      {.file_name = "64mapspreadvals.bin",
       .expected_shape = InteropBitmapShape::kSpreadAcrossKeys},
  };

  for (const auto& test_case : kCases) {
    SCOPED_TRACE(test_case.file_name);
    std::string data = ReadTestResource(test_case.file_name);
    auto result = RoaringPositionBitmap::Deserialize(data);
    ASSERT_THAT(result, IsOk());

    const auto& bitmap = result.value();
    AssertInteropBitmapShape(bitmap, test_case.expected_shape);

    std::set<int64_t> positions;
    bitmap.ForEach([&](int64_t pos) { positions.insert(pos); });
    AssertEqualContent(bitmap, positions);

    auto copy = RoundTripSerialize(bitmap);
    AssertEqualContent(copy, positions);
  }
}

TEST(RoaringPositionBitmapInteropTest, TestDeserializeUnsupportedRoaringExample) {
  // This file is copied from the Apache Iceberg Java repository and it contains
  // a value with key larger than max supported
  std::string data = ReadTestResource("64maphighvals.bin");
  auto result = RoaringPositionBitmap::Deserialize(data);
  ASSERT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  ASSERT_THAT(result, HasErrorMessage("Invalid unsigned key"));
}

}  // namespace iceberg
