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

# Release Process

This guide is for Apache Iceberg committers and PMC members who are managing a release.

## Overview

1. Test the revision to be released
2. Prepare a release candidate (RC) and start a vote
3. Publish the release after the vote passes

## Prerequisites

- You must be an Apache Iceberg committer or PMC member
- Required tools: `git`, `gh` (GitHub CLI), `gpg`, `svn`
- A PGP key for signing (see [Apache release signing guide](https://infra.apache.org/release-signing.html#generate))

### Setting Up Your PGP Key

Your PGP key must be published in the [KEYS file](https://downloads.apache.org/iceberg/KEYS). For first-time release managers:

```bash
# Check out the release distribution directory
svn co https://dist.apache.org/repos/dist/release/iceberg
cd iceberg

# Append your GPG public key
echo "" >> KEYS
gpg --list-sigs <YOUR_KEY_ID> >> KEYS
gpg --armor --export <YOUR_KEY_ID> >> KEYS
svn commit -m "Add GPG key for <YOUR_NAME>"
```

## Step 1: Prepare RC and Vote

Run `release_rc.sh` on a working copy of `apache/iceberg-cpp` (not your fork):

```bash
git clone git@github.com:apache/iceberg-cpp.git && cd iceberg-cpp
GH_TOKEN=${YOUR_GITHUB_TOKEN} dev/release/release_rc.sh ${VERSION} ${RC}
```

For example, to release RC0 of version 0.3.0:

```bash
GH_TOKEN=${YOUR_GITHUB_TOKEN} dev/release/release_rc.sh 0.3.0 0
```

The script will:

1. Tag the release candidate (e.g., `v0.3.0-rc0`)
2. Wait for the GitHub Actions RC workflow to build the source tarball
3. Download and sign the tarball
4. Upload to ASF's dev distribution
5. Print a draft vote email for `dev@iceberg.apache.org`

If an RC has problems, increment the RC number (RC1, RC2, etc.) and repeat.

## Step 2: Publish

After the vote passes (requires 72 hours and at least 3 binding +1 votes), publish the release:

```bash
GH_TOKEN=${YOUR_GITHUB_TOKEN} dev/release/release.sh ${VERSION} ${RC}
```

The script will:

1. Create the final release tag (e.g., `v0.3.0`)
2. Move the RC artifacts from dev to release distribution
3. Create a GitHub Release with the source tarball and signatures
4. Clean up old releases from the distribution directory
5. Print a draft announcement email

After running the script, add the release to [ASF's report database](https://reporter.apache.org/addrelease.html?iceberg).
