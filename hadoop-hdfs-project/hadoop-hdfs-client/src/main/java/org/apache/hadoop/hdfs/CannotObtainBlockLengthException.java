/**
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

package org.apache.hadoop.hdfs;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

import java.io.IOException;

/**
 * This exception is thrown when the the length of a LocatedBlock instance
 * can not be obtained.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class CannotObtainBlockLengthException extends IOException {
  private static final long serialVersionUID = 1L;

  private LocatedBlock locatedBlock;

  /**
   * Constructs an {@code CannotObtainBlockLengthException} with the
   * specified detail message, and the problematic LocatedBlock instance.
   *
   * @param message
   *        The detail message (which is saved for later retrieval
   *        by the {@link #getMessage()} method)
   *
   * @param locatedblock
   *        The LocatedBlock instance which block length can not be obtained
   */
  public CannotObtainBlockLengthException(String message,
      LocatedBlock locatedblock) {
    super(message);
    this.locatedBlock = locatedblock;
  }

  /**
   * Returns the locatedBlock which length can not be obtained.
   * @return The LocatedBlock instance which block length can not be obtained.
   */
  public LocatedBlock getLocatedBlock() {
    return locatedBlock;
  }
}
