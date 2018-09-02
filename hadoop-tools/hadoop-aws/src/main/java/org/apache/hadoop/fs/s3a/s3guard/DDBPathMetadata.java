/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.s3a.s3guard;

import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.QueryOutcome;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.s3a.Tristate;
import org.apache.hadoop.util.Time;

/**
 * {@code DDBPathMetadata} wraps {@link PathMetadata} and adds the
 * * isAuthoritativeDir flag to provide support for authoritative directory
 * listings
 * * lastUpdated field to store the time of the last update of this entry
 * in {@link DynamoDBMetadataStore}.
 */
public class DDBPathMetadata extends PathMetadata {

  private boolean isAuthoritativeDir;
  private long lastUpdated;

  public DDBPathMetadata(PathMetadata pmd) {
    super(pmd.getFileStatus(), pmd.isEmptyDirectory(), pmd.isDeleted());
    this.isAuthoritativeDir = false;
    this.lastUpdated = getInitialLastUpdated();
  }

  public DDBPathMetadata(FileStatus fileStatus) {
    super(fileStatus);
    this.isAuthoritativeDir = false;
    this.lastUpdated = getInitialLastUpdated();
  }

  public DDBPathMetadata(FileStatus fileStatus, Tristate isEmptyDir,
      boolean isDeleted) {
    super(fileStatus, isEmptyDir, isDeleted);
    this.isAuthoritativeDir = false;
    this.lastUpdated = getInitialLastUpdated();
  }

  public DDBPathMetadata(FileStatus fileStatus, Tristate isEmptyDir,
      boolean isDeleted, boolean isAuthoritativeDir) {
    super(fileStatus, isEmptyDir, isDeleted);
    this.isAuthoritativeDir = isAuthoritativeDir;
    this.lastUpdated = getInitialLastUpdated();
  }

  public DDBPathMetadata(FileStatus fileStatus, Tristate isEmptyDir,
      boolean isDeleted, boolean isAuthoritativeDir, long lastUpdated) {
    super(fileStatus, isEmptyDir, isDeleted);
    this.isAuthoritativeDir = isAuthoritativeDir;
    this.lastUpdated = lastUpdated;
  }

  public boolean isAuthoritativeDir() {
    return isAuthoritativeDir;
  }

  public void setAuthoritativeDir(boolean authoritativeDir) {
    isAuthoritativeDir = authoritativeDir;
  }

  @Override
  public boolean equals(Object o) {
    return super.equals(o);
  }

  @Override public int hashCode() {
    return super.hashCode();
  }

  public long getLastUpdated() {
    return lastUpdated;
  }

  public void setLastUpdated(long lastUpdated) {
    this.lastUpdated = lastUpdated;
  }

  void checkIsEmptyDirectory(ItemCollection<QueryOutcome> items) {
    // This class has support for authoritative (fully-cached) directory
    // listings, so we are able to answer TRUE/FALSE here. If the listing is
    // not authoritative, we don't know if we have full listing or not, thus
    // set the UNKNOWN here in that case.
    boolean hasChildren = items.iterator().hasNext();
    if (this.isAuthoritativeDir()) {
      this.setIsEmptyDirectory(
          hasChildren ? Tristate.FALSE : Tristate.TRUE);
    } else {
      this.setIsEmptyDirectory(
          hasChildren ? Tristate.FALSE : Tristate.UNKNOWN);
    }
  }

  private long getInitialLastUpdated() {
    return Time.monotonicNow();
  }

  @Override public String toString() {
    return "DDBPathMetadata{" + "isAuthoritativeDir=" + isAuthoritativeDir
        + ", lastUpdated=" + lastUpdated + ", " + super.toString() + '}';
  }
}
