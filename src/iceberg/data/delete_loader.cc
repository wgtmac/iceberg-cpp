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

#include "iceberg/data/delete_loader.h"

#include <string>
#include <vector>

#include "iceberg/arrow_c_data_guard_internal.h"
#include "iceberg/deletes/position_delete_index.h"
#include "iceberg/file_reader.h"
#include "iceberg/manifest/manifest_entry.h"
#include "iceberg/metadata_columns.h"
#include "iceberg/row/arrow_array_wrapper.h"
#include "iceberg/schema.h"
#include "iceberg/util/macros.h"
#include "iceberg/util/struct_like_set.h"

namespace iceberg {

namespace {

/// \brief Build the projection schema for reading position delete files.
std::shared_ptr<Schema> PosDeleteSchema() {
  return std::make_shared<Schema>(std::vector<SchemaField>{
      MetadataColumns::kDeleteFilePath,
      MetadataColumns::kDeleteFilePos,
  });
}

/// \brief Open a delete file with the given projection schema.
Result<std::unique_ptr<Reader>> OpenDeleteFile(const DataFile& file,
                                               std::shared_ptr<Schema> projection,
                                               const std::shared_ptr<FileIO>& io) {
  ReaderOptions options{
      .path = file.file_path,
      .length = static_cast<size_t>(file.file_size_in_bytes),
      .io = io,
      .projection = std::move(projection),
  };
  return ReaderFactoryRegistry::Open(file.file_format, options);
}

}  // namespace

DeleteLoader::DeleteLoader(std::shared_ptr<FileIO> io) : io_(std::move(io)) {}

DeleteLoader::~DeleteLoader() = default;

Status DeleteLoader::LoadPositionDelete(const DataFile& file, PositionDeleteIndex& index,
                                        std::string_view data_file_path) const {
  // TODO(gangwu): push down path filter to open the file.
  ICEBERG_ASSIGN_OR_RAISE(auto reader, OpenDeleteFile(file, PosDeleteSchema(), io_));

  ICEBERG_ASSIGN_OR_RAISE(auto arrow_schema, reader->Schema());
  internal::ArrowSchemaGuard schema_guard(&arrow_schema);

  while (true) {
    ICEBERG_ASSIGN_OR_RAISE(auto batch_opt, reader->Next());
    if (!batch_opt.has_value()) break;

    auto& batch = batch_opt.value();
    internal::ArrowArrayGuard batch_guard(&batch);

    ICEBERG_ASSIGN_OR_RAISE(
        auto row, ArrowArrayStructLike::Make(arrow_schema, batch, /*row_index=*/0));

    for (int64_t i = 0; i < batch.length; ++i) {
      if (i > 0) {
        ICEBERG_RETURN_UNEXPECTED(row->Reset(i));
      }
      // Field 0: file_path
      ICEBERG_ASSIGN_OR_RAISE(auto path_scalar, row->GetField(0));
      auto path = std::get<std::string_view>(path_scalar);

      if (path == data_file_path) {
        // Field 1: pos
        ICEBERG_ASSIGN_OR_RAISE(auto pos_scalar, row->GetField(1));
        index.Delete(std::get<int64_t>(pos_scalar));
      }
    }
  }

  return reader->Close();
}

Status DeleteLoader::LoadDV(const DataFile& file, PositionDeleteIndex& index) const {
  return NotSupported("Loading deletion vectors is not yet supported");
}

Result<PositionDeleteIndex> DeleteLoader::LoadPositionDeletes(
    std::span<const std::shared_ptr<DataFile>> delete_files,
    std::string_view data_file_path) const {
  PositionDeleteIndex index;

  for (const auto& file : delete_files) {
    if (file->IsDeletionVector()) {
      ICEBERG_RETURN_UNEXPECTED(LoadDV(*file, index));
    }

    ICEBERG_PRECHECK(file->content == DataFile::Content::kPositionDeletes,
                     "Expected position delete file but got content type {}",
                     static_cast<int>(file->content));

    ICEBERG_RETURN_UNEXPECTED(LoadPositionDelete(*file, index, data_file_path));
  }

  return index;
}

Result<std::unique_ptr<UncheckedStructLikeSet>> DeleteLoader::LoadEqualityDeletes(
    std::span<const std::shared_ptr<DataFile>> delete_files,
    const StructType& equality_type) const {
  auto eq_set = std::make_unique<UncheckedStructLikeSet>(equality_type);

  std::shared_ptr<Schema> projection = equality_type.ToSchema();

  for (const auto& file : delete_files) {
    ICEBERG_PRECHECK(file->content == DataFile::Content::kEqualityDeletes,
                     "Expected equality delete file but got content type {}",
                     static_cast<int>(file->content));

    ICEBERG_ASSIGN_OR_RAISE(auto reader, OpenDeleteFile(*file, projection, io_));
    ICEBERG_ASSIGN_OR_RAISE(auto arrow_schema, reader->Schema());
    internal::ArrowSchemaGuard schema_guard(&arrow_schema);

    while (true) {
      ICEBERG_ASSIGN_OR_RAISE(auto batch_opt, reader->Next());
      if (!batch_opt.has_value()) break;

      auto& batch = batch_opt.value();
      internal::ArrowArrayGuard batch_guard(&batch);

      ICEBERG_ASSIGN_OR_RAISE(
          auto row, ArrowArrayStructLike::Make(arrow_schema, batch, /*row_index=*/0));

      for (int64_t i = 0; i < batch.length; ++i) {
        if (i > 0) {
          ICEBERG_RETURN_UNEXPECTED(row->Reset(i));
        }
        ICEBERG_RETURN_UNEXPECTED(eq_set->Insert(*row));
      }
    }

    ICEBERG_RETURN_UNEXPECTED(reader->Close());
  }

  return eq_set;
}

}  // namespace iceberg
