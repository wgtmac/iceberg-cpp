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

#include <arrow/array.h>
#include <arrow/c/bridge.h>
#include <arrow/json/from_string.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include "iceberg/arrow/arrow_fs_file_io_internal.h"
#include "iceberg/arrow/arrow_status_internal.h"
#include "iceberg/file_reader.h"
#include "iceberg/file_writer.h"
#include "iceberg/parquet/parquet_register.h"
#include "iceberg/schema_internal.h"
#include "iceberg/test/matchers.h"
#include "iceberg/type.h"

namespace iceberg::parquet {

namespace {

Status WriteArray(std::shared_ptr<::arrow::Array> data,
                  const WriterOptions& writer_options) {
  ICEBERG_ASSIGN_OR_RAISE(
      auto writer, WriterFactoryRegistry::Open(FileFormatType::kParquet, writer_options));
  ArrowArray arr;
  ICEBERG_ARROW_RETURN_NOT_OK(::arrow::ExportArray(*data, &arr));
  ICEBERG_RETURN_UNEXPECTED(writer->Write(&arr));
  return writer->Close();
}

}  // namespace

class ParquetReaderNoProjectionTest : public ::testing::Test {
 protected:
  static void SetUpTestSuite() { parquet::RegisterAll(); }

  void SetUp() override {
    file_io_ = arrow::ArrowFileSystemFileIO::MakeMockFileIO();
    temp_parquet_file_ = "parquet_reader_no_projection_test.parquet";
  }

  void CreateSimpleParquetFile() {
    auto schema = std::make_shared<Schema>(
        std::vector<SchemaField>{SchemaField::MakeRequired(1, "id", int32()),
                                 SchemaField::MakeOptional(2, "name", string())});

    ArrowSchema arrow_c_schema;
    ASSERT_THAT(ToArrowSchema(*schema, &arrow_c_schema), IsOk());
    auto arrow_schema = ::arrow::ImportType(&arrow_c_schema).ValueOrDie();

    auto array =
        ::arrow::json::ArrayFromJSONString(::arrow::struct_(arrow_schema->fields()),
                                           R"([[1, "Foo"],[2, "Bar"],[3, "Baz"]])")
            .ValueOrDie();

    WriterProperties writer_properties;
    writer_properties.Set(WriterProperties::kParquetCompression,
                          std::string("uncompressed"));

    ASSERT_TRUE(WriteArray(array, {.path = temp_parquet_file_,
                                   .schema = schema,
                                   .io = file_io_,
                                   .properties = std::move(writer_properties)}));
  }

  void VerifyNextBatch(Reader& reader, std::string_view expected_json) {
    auto schema_result = reader.Schema();
    ASSERT_THAT(schema_result, IsOk());
    auto arrow_c_schema = std::move(schema_result.value());
    auto import_schema_result = ::arrow::ImportType(&arrow_c_schema);
    auto arrow_schema = import_schema_result.ValueOrDie();

    auto data = reader.Next();
    ASSERT_THAT(data, IsOk()) << "Reader.Next() failed: " << data.error().message;
    ASSERT_TRUE(data.value().has_value()) << "Reader.Next() returned no data";
    auto arrow_c_array = data.value().value();
    auto data_result = ::arrow::ImportArray(&arrow_c_array, arrow_schema);
    auto arrow_array = data_result.ValueOrDie();

    auto expected_array =
        ::arrow::json::ArrayFromJSONString(arrow_schema, expected_json).ValueOrDie();
    ASSERT_TRUE(arrow_array->Equals(*expected_array));
  }

  void VerifyExhausted(Reader& reader) {
    auto data = reader.Next();
    ASSERT_THAT(data, IsOk());
    ASSERT_FALSE(data.value().has_value());
  }

  std::shared_ptr<FileIO> file_io_;
  std::string temp_parquet_file_;
};

TEST_F(ParquetReaderNoProjectionTest, ReadWithoutProjection) {
  CreateSimpleParquetFile();

  // No projection passed
  auto reader_result = ReaderFactoryRegistry::Open(
      FileFormatType::kParquet, {.path = temp_parquet_file_, .io = file_io_});

  // This is expected to fail currently
  ASSERT_THAT(reader_result, IsOk())
      << "Failed to create reader: " << reader_result.error().message;
  auto reader = std::move(reader_result.value());

  ASSERT_NO_FATAL_FAILURE(
      VerifyNextBatch(*reader, R"([[1, "Foo"], [2, "Bar"], [3, "Baz"]])"));
  ASSERT_NO_FATAL_FAILURE(VerifyExhausted(*reader));
}

}  // namespace iceberg::parquet
