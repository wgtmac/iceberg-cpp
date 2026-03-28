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

# Documentation

The website is built with [MkDocs](https://www.mkdocs.org/) (Material theme) and [Doxygen](https://www.doxygen.nl/) for API docs, hosted at https://cpp.iceberg.apache.org/.

## Prerequisites

- Python 3 with pip
- [Doxygen](https://www.doxygen.nl/)

## Build

From the project root:

```bash
make install-deps    # Install Python dependencies
make build-api-docs  # Generate API docs with Doxygen
make build-docs      # Build MkDocs site
make all             # Build everything
```

## Local Preview

```bash
make serve-docs      # Serve at http://127.0.0.1:8000
```

## Clean

```bash
make clean-docs      # Remove generated files
```
