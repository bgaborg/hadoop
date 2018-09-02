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
import com.amazonaws.services.dynamodbv2.document.internal.IteratorSupport;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.Tristate;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.*;

public class TestDDBPathMetadata {

  @Test
  public void testCheckIsEmptyDirectory() {
    PathMetadata pmd = new PathMetadata(getTestFileStatus(new Path("/")));
    DDBPathMetadata ddbPathMetadata = new DDBPathMetadata(pmd);

    ItemCollection items = Mockito.mock(ItemCollection.class);
    IteratorSupport iterator = Mockito.mock(IteratorSupport.class);

    Mockito.when(items.iterator()).thenReturn(iterator);

    ddbPathMetadata.setAuthoritativeDir(true);
    Mockito.when(iterator.hasNext()).thenReturn(true);
    ddbPathMetadata.checkIsEmptyDirectory(items);
    assertEquals(Tristate.FALSE, ddbPathMetadata.isEmptyDirectory());

    ddbPathMetadata.setAuthoritativeDir(true);
    Mockito.when(iterator.hasNext()).thenReturn(false);
    ddbPathMetadata.checkIsEmptyDirectory(items);
    assertEquals(Tristate.TRUE, ddbPathMetadata.isEmptyDirectory());

    ddbPathMetadata.setAuthoritativeDir(false);
    Mockito.when(iterator.hasNext()).thenReturn(true);
    ddbPathMetadata.checkIsEmptyDirectory(items);
    assertEquals(Tristate.FALSE, ddbPathMetadata.isEmptyDirectory());

    ddbPathMetadata.setAuthoritativeDir(false);
    Mockito.when(iterator.hasNext()).thenReturn(false);
    ddbPathMetadata.checkIsEmptyDirectory(items);
    assertEquals(Tristate.UNKNOWN, ddbPathMetadata.isEmptyDirectory());
  }

  private FileStatus getTestFileStatus(Path p) {
    return new FileStatus(100, false, 1, 1, System.currentTimeMillis(), p);
  }

}