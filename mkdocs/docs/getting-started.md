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

# Getting Started

## Requirements

- CMake 3.25 or higher
- C++23 compliant compiler

## Customizing Dependency URLs

If you experience network issues when downloading dependencies, you can customize the download URLs using environment variables.

The following environment variables can be set to customize dependency URLs:

- `ICEBERG_ARROW_URL`: Apache Arrow tarball URL
- `ICEBERG_AVRO_URL`: Apache Avro tarball URL
- `ICEBERG_AVRO_GIT_URL`: Apache Avro git repository URL
- `ICEBERG_NANOARROW_URL`: Nanoarrow tarball URL
- `ICEBERG_CROARING_URL`: CRoaring tarball URL
- `ICEBERG_NLOHMANN_JSON_URL`: nlohmann-json tarball URL
- `ICEBERG_CPR_URL`: cpr tarball URL

Example usage:

```bash
export ICEBERG_ARROW_URL="https://your-mirror.com/apache-arrow-22.0.0.tar.gz"
cmake -S . -B build
```

## Build

### Build, Run Test and Install Core Libraries

```bash
cd iceberg-cpp
cmake -S . -B build -G Ninja -DCMAKE_INSTALL_PREFIX=/path/to/install -DICEBERG_BUILD_STATIC=ON -DICEBERG_BUILD_SHARED=ON
cmake --build build
ctest --test-dir build --output-on-failure
cmake --install build
```

### Build and Install Iceberg Bundle Library

#### Vendored Apache Arrow (default)

```bash
cmake -S . -B build -G Ninja -DCMAKE_INSTALL_PREFIX=/path/to/install -DICEBERG_BUILD_BUNDLE=ON
cmake --build build
ctest --test-dir build --output-on-failure
cmake --install build
```

#### Provided Apache Arrow

```bash
cmake -S . -B build -G Ninja -DCMAKE_INSTALL_PREFIX=/path/to/install -DCMAKE_PREFIX_PATH=/path/to/arrow -DICEBERG_BUILD_BUNDLE=ON
cmake --build build
ctest --test-dir build --output-on-failure
cmake --install build
```

### Build Examples

After installing the core libraries, you can build the examples:

```bash
cd iceberg-cpp/example
cmake -S . -B build -G Ninja -DCMAKE_PREFIX_PATH=/path/to/install
cmake --build build
```

If you are using provided Apache Arrow, you need to include `/path/to/arrow` in `CMAKE_PREFIX_PATH` as below.

```bash
cmake -S . -B build -G Ninja -DCMAKE_PREFIX_PATH="/path/to/install;/path/to/arrow"
```
