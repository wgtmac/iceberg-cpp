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
    auto result = bitmap.Contains(pos);
    ASSERT_THAT(result, IsOk()) << "Error for pos: " << pos;
    ASSERT_TRUE(result.value()) << "Missing position: " << pos;
  }
  bitmap.ForEach([&](int64_t pos) {
    ASSERT_TRUE(positions.count(pos) > 0) << "Unexpected position: " << pos;
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

}  // namespace

TEST(RoaringPositionBitmapTest, TestAdd) {
  RoaringPositionBitmap bitmap;

  EXPECT_THAT(bitmap.Add(10L), IsOk());
  EXPECT_TRUE(bitmap.Contains(10L).value());

  EXPECT_THAT(bitmap.Add(0L), IsOk());
  EXPECT_TRUE(bitmap.Contains(0L).value());

  EXPECT_THAT(bitmap.Add(10L), IsOk());  // duplicate
  EXPECT_TRUE(bitmap.Contains(10L).value());
}

TEST(RoaringPositionBitmapTest, TestAddPositionsRequiringMultipleBitmaps) {
  RoaringPositionBitmap bitmap;

  int64_t pos1 = (static_cast<int64_t>(0) << 32) | 10L;
  int64_t pos2 = (static_cast<int64_t>(1) << 32) | 20L;
  int64_t pos3 = (static_cast<int64_t>(2) << 32) | 30L;
  int64_t pos4 = (static_cast<int64_t>(100) << 32) | 40L;

  EXPECT_THAT(bitmap.Add(pos1), IsOk());
  EXPECT_THAT(bitmap.Add(pos2), IsOk());
  EXPECT_THAT(bitmap.Add(pos3), IsOk());
  EXPECT_THAT(bitmap.Add(pos4), IsOk());

  EXPECT_TRUE(bitmap.Contains(pos1).value());
  EXPECT_TRUE(bitmap.Contains(pos2).value());
  EXPECT_TRUE(bitmap.Contains(pos3).value());
  EXPECT_TRUE(bitmap.Contains(pos4).value());
  EXPECT_EQ(bitmap.Cardinality(), 4);
  EXPECT_GT(bitmap.SerializedSizeInBytes(), 4);
}

TEST(RoaringPositionBitmapTest, TestAddRange) {
  RoaringPositionBitmap bitmap;

  int64_t start = 10;
  int64_t end = 20;
  EXPECT_THAT(bitmap.AddRange(start, end), IsOk());

  for (int64_t pos = start; pos < end; ++pos) {
    EXPECT_TRUE(bitmap.Contains(pos).value());
  }
  EXPECT_FALSE(bitmap.Contains(9).value());
  EXPECT_FALSE(bitmap.Contains(20).value());
  EXPECT_EQ(bitmap.Cardinality(), 10);
}

TEST(RoaringPositionBitmapTest, TestAddRangeAcrossKeys) {
  RoaringPositionBitmap bitmap;

  int64_t start = (static_cast<int64_t>(1) << 32) - 5;
  int64_t end = (static_cast<int64_t>(1) << 32) + 5;
  EXPECT_THAT(bitmap.AddRange(start, end), IsOk());

  for (int64_t pos = start; pos < end; ++pos) {
    EXPECT_TRUE(bitmap.Contains(pos).value());
  }
  EXPECT_FALSE(bitmap.Contains(0).value());
  EXPECT_FALSE(bitmap.Contains(end).value());
  EXPECT_EQ(bitmap.Cardinality(), 10);
}

TEST(RoaringPositionBitmapTest, TestAddEmptyRange) {
  RoaringPositionBitmap bitmap;
  EXPECT_THAT(bitmap.AddRange(10, 10), IsOk());
  EXPECT_TRUE(bitmap.IsEmpty());
}

TEST(RoaringPositionBitmapTest, TestOr) {
  RoaringPositionBitmap bitmap1;
  EXPECT_THAT(bitmap1.Add(10L), IsOk());
  EXPECT_THAT(bitmap1.Add(20L), IsOk());

  RoaringPositionBitmap bitmap2;
  EXPECT_THAT(bitmap2.Add(30L), IsOk());
  EXPECT_THAT(bitmap2.Add(40L), IsOk());
  EXPECT_THAT(bitmap2.Add(static_cast<int64_t>(2) << 32), IsOk());

  bitmap1.Or(bitmap2);

  EXPECT_TRUE(bitmap1.Contains(10L).value());
  EXPECT_TRUE(bitmap1.Contains(20L).value());
  EXPECT_TRUE(bitmap1.Contains(30L).value());
  EXPECT_TRUE(bitmap1.Contains(40L).value());
  EXPECT_TRUE(bitmap1.Contains(static_cast<int64_t>(2) << 32).value());
  EXPECT_EQ(bitmap1.Cardinality(), 5);

  // bitmap2 should be unchanged
  EXPECT_FALSE(bitmap2.Contains(10L).value());
  EXPECT_FALSE(bitmap2.Contains(20L).value());
  EXPECT_EQ(bitmap2.Cardinality(), 3);
}

TEST(RoaringPositionBitmapTest, TestOrWithEmptyBitmap) {
  RoaringPositionBitmap bitmap1;
  EXPECT_THAT(bitmap1.Add(10L), IsOk());
  EXPECT_THAT(bitmap1.Add(20L), IsOk());

  RoaringPositionBitmap empty_bitmap;
  bitmap1.Or(empty_bitmap);

  EXPECT_TRUE(bitmap1.Contains(10L).value());
  EXPECT_TRUE(bitmap1.Contains(20L).value());
  EXPECT_EQ(bitmap1.Cardinality(), 2);

  EXPECT_FALSE(empty_bitmap.Contains(10L).value());
  EXPECT_FALSE(empty_bitmap.Contains(20L).value());
  EXPECT_EQ(empty_bitmap.Cardinality(), 0);
  EXPECT_TRUE(empty_bitmap.IsEmpty());
}

TEST(RoaringPositionBitmapTest, TestOrWithOverlapping) {
  RoaringPositionBitmap bitmap1;
  EXPECT_THAT(bitmap1.Add(10L), IsOk());
  EXPECT_THAT(bitmap1.Add(20L), IsOk());
  EXPECT_THAT(bitmap1.Add(30L), IsOk());

  RoaringPositionBitmap bitmap2;
  EXPECT_THAT(bitmap2.Add(20L), IsOk());
  EXPECT_THAT(bitmap2.Add(40L), IsOk());

  bitmap1.Or(bitmap2);

  EXPECT_TRUE(bitmap1.Contains(10L).value());
  EXPECT_TRUE(bitmap1.Contains(20L).value());
  EXPECT_TRUE(bitmap1.Contains(30L).value());
  EXPECT_TRUE(bitmap1.Contains(40L).value());
  EXPECT_EQ(bitmap1.Cardinality(), 4);

  EXPECT_FALSE(bitmap2.Contains(10L).value());
  EXPECT_TRUE(bitmap2.Contains(20L).value());
  EXPECT_FALSE(bitmap2.Contains(30L).value());
  EXPECT_TRUE(bitmap2.Contains(40L).value());
  EXPECT_EQ(bitmap2.Cardinality(), 2);
}

TEST(RoaringPositionBitmapTest, TestOrSparseBitmaps) {
  RoaringPositionBitmap bitmap1;
  EXPECT_THAT(bitmap1.Add((static_cast<int64_t>(0) << 32) | 100L), IsOk());
  EXPECT_THAT(bitmap1.Add((static_cast<int64_t>(1) << 32) | 200L), IsOk());

  RoaringPositionBitmap bitmap2;
  EXPECT_THAT(bitmap2.Add((static_cast<int64_t>(2) << 32) | 300L), IsOk());
  EXPECT_THAT(bitmap2.Add((static_cast<int64_t>(3) << 32) | 400L), IsOk());

  bitmap1.Or(bitmap2);

  EXPECT_TRUE(bitmap1.Contains((static_cast<int64_t>(0) << 32) | 100L).value());
  EXPECT_TRUE(bitmap1.Contains((static_cast<int64_t>(1) << 32) | 200L).value());
  EXPECT_TRUE(bitmap1.Contains((static_cast<int64_t>(2) << 32) | 300L).value());
  EXPECT_TRUE(bitmap1.Contains((static_cast<int64_t>(3) << 32) | 400L).value());
  EXPECT_EQ(bitmap1.Cardinality(), 4);
}

TEST(RoaringPositionBitmapTest, TestCardinality) {
  RoaringPositionBitmap bitmap;

  EXPECT_EQ(bitmap.Cardinality(), 0);

  EXPECT_THAT(bitmap.Add(10L), IsOk());
  EXPECT_THAT(bitmap.Add(20L), IsOk());
  EXPECT_THAT(bitmap.Add(30L), IsOk());
  EXPECT_EQ(bitmap.Cardinality(), 3);

  EXPECT_THAT(bitmap.Add(10L), IsOk());  // already exists
  EXPECT_EQ(bitmap.Cardinality(), 3);
}

TEST(RoaringPositionBitmapTest, TestCardinalitySparseBitmaps) {
  RoaringPositionBitmap bitmap;

  EXPECT_THAT(bitmap.Add((static_cast<int64_t>(0) << 32) | 100L), IsOk());
  EXPECT_THAT(bitmap.Add((static_cast<int64_t>(0) << 32) | 101L), IsOk());
  EXPECT_THAT(bitmap.Add((static_cast<int64_t>(0) << 32) | 105L), IsOk());
  EXPECT_THAT(bitmap.Add((static_cast<int64_t>(1) << 32) | 200L), IsOk());
  EXPECT_THAT(bitmap.Add((static_cast<int64_t>(100) << 32) | 300L), IsOk());

  EXPECT_EQ(bitmap.Cardinality(), 5);
}

TEST(RoaringPositionBitmapTest, TestSerializeDeserializeRoundTrip) {
  RoaringPositionBitmap bitmap;
  EXPECT_THAT(bitmap.Add(10L), IsOk());
  EXPECT_THAT(bitmap.Add(20L), IsOk());
  EXPECT_THAT(bitmap.Add((static_cast<int64_t>(1) << 32) | 30L), IsOk());

  auto copy = RoundTripSerialize(bitmap);

  EXPECT_EQ(copy.Cardinality(), bitmap.Cardinality());
  EXPECT_TRUE(copy.Contains(10L).value());
  EXPECT_TRUE(copy.Contains(20L).value());
  EXPECT_TRUE(copy.Contains((static_cast<int64_t>(1) << 32) | 30L).value());
}

TEST(RoaringPositionBitmapTest, TestSerializeDeserializeEmpty) {
  RoaringPositionBitmap bitmap;
  auto copy = RoundTripSerialize(bitmap);
  EXPECT_TRUE(copy.IsEmpty());
  EXPECT_EQ(copy.Cardinality(), 0);
}

TEST(RoaringPositionBitmapTest, TestSerializeDeserializeAllContainerBitmap) {
  RoaringPositionBitmap bitmap;

  // bitmap 0, container 0 (array - few elements)
  EXPECT_THAT(bitmap.Add(Position(0, 0, 5)), IsOk());
  EXPECT_THAT(bitmap.Add(Position(0, 0, 7)), IsOk());

  // bitmap 0, container 1 (array that can be compressed)
  EXPECT_THAT(bitmap.AddRange(Position(0, 1, 1), Position(0, 1, 1000)), IsOk());

  // bitmap 0, container 2 (bitset - nearly full container)
  EXPECT_THAT(bitmap.AddRange(Position(0, 2, 1), Position(0, 2, kContainerOffset - 1)),
              IsOk());

  // bitmap 1, container 0 (array)
  EXPECT_THAT(bitmap.Add(Position(1, 0, 10)), IsOk());
  EXPECT_THAT(bitmap.Add(Position(1, 0, 20)), IsOk());

  // bitmap 1, container 1 (array that can be compressed)
  EXPECT_THAT(bitmap.AddRange(Position(1, 1, 10), Position(1, 1, 500)), IsOk());

  // bitmap 1, container 2 (bitset)
  EXPECT_THAT(bitmap.AddRange(Position(1, 2, 1), Position(1, 2, kContainerOffset - 1)),
              IsOk());

  EXPECT_TRUE(bitmap.Optimize());

  auto copy = RoundTripSerialize(bitmap);

  EXPECT_EQ(copy.Cardinality(), bitmap.Cardinality());
  copy.ForEach([&](int64_t pos) { EXPECT_TRUE(bitmap.Contains(pos).value()); });
  bitmap.ForEach([&](int64_t pos) { EXPECT_TRUE(copy.Contains(pos).value()); });
}

TEST(RoaringPositionBitmapTest, TestForEach) {
  RoaringPositionBitmap bitmap;
  EXPECT_THAT(bitmap.Add(30L), IsOk());
  EXPECT_THAT(bitmap.Add(10L), IsOk());
  EXPECT_THAT(bitmap.Add(20L), IsOk());
  EXPECT_THAT(bitmap.Add((static_cast<int64_t>(1) << 32) | 5L), IsOk());

  std::vector<int64_t> positions;
  bitmap.ForEach([&](int64_t pos) { positions.push_back(pos); });

  ASSERT_EQ(positions.size(), 4u);
  EXPECT_EQ(positions[0], 10L);
  EXPECT_EQ(positions[1], 20L);
  EXPECT_EQ(positions[2], 30L);
  EXPECT_EQ(positions[3], (static_cast<int64_t>(1) << 32) | 5L);
}

TEST(RoaringPositionBitmapTest, TestIsEmpty) {
  RoaringPositionBitmap bitmap;
  EXPECT_TRUE(bitmap.IsEmpty());

  EXPECT_THAT(bitmap.Add(10L), IsOk());
  EXPECT_FALSE(bitmap.IsEmpty());
}

TEST(RoaringPositionBitmapTest, TestOptimize) {
  RoaringPositionBitmap bitmap;
  EXPECT_THAT(bitmap.AddRange(0, 10000), IsOk());

  bool changed = bitmap.Optimize();
  EXPECT_TRUE(changed);

  // Content should be unchanged after RLE optimization
  EXPECT_EQ(bitmap.Cardinality(), 10000);
  for (int64_t i = 0; i < 10000; ++i) {
    EXPECT_TRUE(bitmap.Contains(i).value());
  }

  // Round-trip should preserve content after RLE
  auto copy = RoundTripSerialize(bitmap);
  EXPECT_EQ(copy.Cardinality(), 10000);
}

TEST(RoaringPositionBitmapTest, TestUnsupportedPositions) {
  RoaringPositionBitmap bitmap;

  // Negative position
  EXPECT_THAT(bitmap.Add(-1L), IsError(ErrorKind::kInvalidArgument));

  // Contains with negative position
  EXPECT_THAT(bitmap.Contains(-1L), IsError(ErrorKind::kInvalidArgument));

  // Position exceeding MAX_POSITION
  EXPECT_THAT(bitmap.Add(RoaringPositionBitmap::kMaxPosition + 1L),
              IsError(ErrorKind::kInvalidArgument));

  // Contains with position exceeding MAX_POSITION
  EXPECT_THAT(bitmap.Contains(RoaringPositionBitmap::kMaxPosition + 1L),
              IsError(ErrorKind::kInvalidArgument));
}

TEST(RoaringPositionBitmapTest, TestDeserializeSupportedRoaringExamples) {
  for (const auto& file :
       {"64map32bitvals.bin", "64mapempty.bin", "64mapspreadvals.bin"}) {
    std::string data = ReadTestResource(file);
    auto result = RoaringPositionBitmap::Deserialize(data);
    EXPECT_THAT(RoaringPositionBitmap::Deserialize(data), IsOk());
  }
}

TEST(RoaringPositionBitmapTest, TestDeserializeUnsupportedRoaringExample) {
  // This file contains a value with key larger than max supported
  std::string data = ReadTestResource("64maphighvals.bin");
  auto result = RoaringPositionBitmap::Deserialize(data);
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
  EXPECT_THAT(result, HasErrorMessage("Invalid unsigned key"));
}

TEST(RoaringPositionBitmapTest, TestRandomSparseBitmap) {
  std::mt19937_64 rng(42);
  RoaringPositionBitmap bitmap;
  std::set<int64_t> positions;

  std::uniform_int_distribution<int64_t> dist(0, static_cast<int64_t>(5) << 32);

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
    EXPECT_EQ(result.value(), positions.count(pos) > 0);
  }
}

TEST(RoaringPositionBitmapTest, TestRandomDenseBitmap) {
  RoaringPositionBitmap bitmap;
  std::set<int64_t> positions;

  // Create dense ranges across multiple bitmap keys
  for (int64_t offset : {static_cast<int64_t>(0), static_cast<int64_t>(2) << 32,
                         static_cast<int64_t>(5) << 32}) {
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
  std::uniform_int_distribution<int64_t> dist1(static_cast<int64_t>(3) << 32,
                                               static_cast<int64_t>(5) << 32);
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
  std::uniform_int_distribution<int64_t> dist2(0, static_cast<int64_t>(1) << 32);
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
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));

  // Invalid bitmap count (very large)
  std::string buf(8, '\xFF');
  result = RoaringPositionBitmap::Deserialize(buf);
  EXPECT_THAT(result, IsError(ErrorKind::kInvalidArgument));
}

}  // namespace iceberg
