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
package org.apache.iceberg.exceptions;

import com.google.errorprone.annotations.FormatMethod;

/**
 * Exception thrown when an idempotency key conflicts with a different operation. This indicates the
 * same key was used for different operations, which is not allowed. HTTP status code: 409 Conflict
 */
public class IdempotencyKeyConflictException extends RESTException implements CleanableFailure {
  @FormatMethod
  public IdempotencyKeyConflictException(String message, Object... args) {
    super(message, args);
  }

  @FormatMethod
  public IdempotencyKeyConflictException(Throwable cause, String message, Object... args) {
    super(cause, message, args);
  }
}
