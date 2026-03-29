<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Release History

| Version | Date | Links |
|---------|------|-------|
| 0.2.0 | January 26, 2026 | [Release Notes](https://github.com/apache/iceberg-cpp/releases/tag/v0.2.0) · [Source](https://dist.apache.org/repos/dist/release/iceberg/apache-iceberg-cpp-0.2.0/) · [Blog Post](https://iceberg.apache.org/blog/apache-iceberg-cpp-0.2.0-release/) |
| 0.1.0 | September 10, 2025 | [Release Notes](https://github.com/apache/iceberg-cpp/releases/tag/v0.1.0) · [Source](https://archive.apache.org/dist/iceberg/apache-iceberg-cpp-0.1.0/) |

For the full changelog of each release, see the [GitHub Releases page](https://github.com/apache/iceberg-cpp/releases).

## 0.2.0

- Table scan planning with V2 delete and filtering support
- Append table support
- Schema evolution and table metadata update operations
- Transaction API with snapshot management
- REST catalog client with namespace and table CRUD
- Expression system with metrics and residual evaluators
- Meson build system support

## 0.1.0

- Core data types, schema, and table metadata (JSON serde)
- Partition specs, sort orders, and snapshot management
- Basic table scan planning (w/o deletes)
- Avro and Parquet file format support
- Local file I/O via Arrow FileSystem
- In-memory catalog
