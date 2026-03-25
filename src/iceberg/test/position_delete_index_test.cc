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

#include "iceberg/deletes/position_delete_index.h"

#include <gtest/gtest.h>

namespace iceberg {

TEST(PositionDeleteIndexTest, TestEmptyIndex) {
  PositionDeleteIndex index;
  ASSERT_TRUE(index.IsEmpty());
  ASSERT_EQ(index.Cardinality(), 0);
  ASSERT_FALSE(index.IsDeleted(0));
  ASSERT_FALSE(index.IsDeleted(100));
}

TEST(PositionDeleteIndexTest, TestSingleDelete) {
  PositionDeleteIndex index;
  index.Delete(42);

  ASSERT_FALSE(index.IsEmpty());
  ASSERT_EQ(index.Cardinality(), 1);
  ASSERT_TRUE(index.IsDeleted(42));
  ASSERT_FALSE(index.IsDeleted(41));
  ASSERT_FALSE(index.IsDeleted(43));
}

TEST(PositionDeleteIndexTest, TestMultipleDeletes) {
  PositionDeleteIndex index;
  index.Delete(10);
  index.Delete(20);
  index.Delete(30);

  ASSERT_EQ(index.Cardinality(), 3);
  ASSERT_TRUE(index.IsDeleted(10));
  ASSERT_TRUE(index.IsDeleted(20));
  ASSERT_TRUE(index.IsDeleted(30));
  ASSERT_FALSE(index.IsDeleted(15));
}

TEST(PositionDeleteIndexTest, TestDeleteRange) {
  PositionDeleteIndex index;
  index.Delete(10, 15);

  ASSERT_EQ(index.Cardinality(), 5);
  ASSERT_FALSE(index.IsDeleted(9));
  ASSERT_TRUE(index.IsDeleted(10));
  ASSERT_TRUE(index.IsDeleted(11));
  ASSERT_TRUE(index.IsDeleted(12));
  ASSERT_TRUE(index.IsDeleted(13));
  ASSERT_TRUE(index.IsDeleted(14));
  ASSERT_FALSE(index.IsDeleted(15));
}

TEST(PositionDeleteIndexTest, TestIsDeleted) {
  PositionDeleteIndex index;
  index.Delete(5);
  index.Delete(100);
  index.Delete(1000);

  ASSERT_TRUE(index.IsDeleted(5));
  ASSERT_TRUE(index.IsDeleted(100));
  ASSERT_TRUE(index.IsDeleted(1000));
  ASSERT_FALSE(index.IsDeleted(0));
  ASSERT_FALSE(index.IsDeleted(50));
  ASSERT_FALSE(index.IsDeleted(500));
}

TEST(PositionDeleteIndexTest, TestCardinality) {
  PositionDeleteIndex index;
  ASSERT_EQ(index.Cardinality(), 0);

  index.Delete(1);
  ASSERT_EQ(index.Cardinality(), 1);

  index.Delete(2);
  index.Delete(3);
  ASSERT_EQ(index.Cardinality(), 3);

  index.Delete(10, 20);
  ASSERT_EQ(index.Cardinality(), 13);
}

TEST(PositionDeleteIndexTest, TestMerge) {
  PositionDeleteIndex index1;
  index1.Delete(10);
  index1.Delete(20);

  PositionDeleteIndex index2;
  index2.Delete(20);
  index2.Delete(30);

  index1.Merge(index2);

  ASSERT_EQ(index1.Cardinality(), 3);
  ASSERT_TRUE(index1.IsDeleted(10));
  ASSERT_TRUE(index1.IsDeleted(20));
  ASSERT_TRUE(index1.IsDeleted(30));
}

TEST(PositionDeleteIndexTest, TestMergeEmpty) {
  PositionDeleteIndex index1;
  index1.Delete(10);

  PositionDeleteIndex index2;

  index1.Merge(index2);
  ASSERT_EQ(index1.Cardinality(), 1);
  ASSERT_TRUE(index1.IsDeleted(10));

  PositionDeleteIndex index3;
  PositionDeleteIndex index4;
  index4.Delete(20);

  index3.Merge(index4);
  ASSERT_EQ(index3.Cardinality(), 1);
  ASSERT_TRUE(index3.IsDeleted(20));
}

TEST(PositionDeleteIndexTest, TestInvalidPositions) {
  PositionDeleteIndex index;

  index.Delete(-1);
  index.Delete(-100);

  ASSERT_TRUE(index.IsEmpty());
  ASSERT_FALSE(index.IsDeleted(-1));
  ASSERT_FALSE(index.IsDeleted(-100));
}

TEST(PositionDeleteIndexTest, TestLargePositions) {
  PositionDeleteIndex index;

  int64_t large_pos = (int64_t{1} << 32) | 100;
  index.Delete(large_pos);

  ASSERT_EQ(index.Cardinality(), 1);
  ASSERT_TRUE(index.IsDeleted(large_pos));
  ASSERT_FALSE(index.IsDeleted(large_pos - 1));
  ASSERT_FALSE(index.IsDeleted(large_pos + 1));
}

TEST(PositionDeleteIndexTest, TestOverlappingRanges) {
  PositionDeleteIndex index;

  index.Delete(10, 20);
  index.Delete(15, 25);

  ASSERT_EQ(index.Cardinality(), 15);
  ASSERT_FALSE(index.IsDeleted(9));
  ASSERT_TRUE(index.IsDeleted(10));
  ASSERT_TRUE(index.IsDeleted(15));
  ASSERT_TRUE(index.IsDeleted(19));
  ASSERT_TRUE(index.IsDeleted(24));
  ASSERT_FALSE(index.IsDeleted(25));
}

TEST(PositionDeleteIndexTest, TestIdempotence) {
  PositionDeleteIndex index;

  index.Delete(42);
  index.Delete(42);
  index.Delete(42);

  ASSERT_EQ(index.Cardinality(), 1);
  ASSERT_TRUE(index.IsDeleted(42));
}

TEST(PositionDeleteIndexTest, TestMergeIdempotence) {
  PositionDeleteIndex index1;
  index1.Delete(10);
  index1.Delete(20);

  PositionDeleteIndex index2;
  index2.Delete(10);
  index2.Delete(20);

  index1.Merge(index2);
  index1.Merge(index2);

  ASSERT_EQ(index1.Cardinality(), 2);
  ASSERT_TRUE(index1.IsDeleted(10));
  ASSERT_TRUE(index1.IsDeleted(20));
}

}  // namespace iceberg
