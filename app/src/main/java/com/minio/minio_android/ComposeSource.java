/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage,
 * (C) 2019 MinIO, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.minio.minio_android;

import java.util.Map;
import java.util.TreeMap;
import com.minio.minio_android.errors.InvalidArgumentException;
import com.minio.minio_android.CopyConditions;

public class ComposeSource {
  private String bucketName;
  private String objectName;
  private Long offset;
  private Long length;
  private Map<String, String> headerMap;
  private CopyConditions copyConditions;
  private ServerSideEncryption sse;
  private long objectSize;
  private Map<String, String> headers;

  /**
   * Create new ComposeSource for given bucket and object.
   */
  public ComposeSource(String bucketName, String objectName) throws InvalidArgumentException {
    this(bucketName, objectName, null, null, null, null, null);
  }

  /**
   * Create new ComposeSource for given bucket, object, offset and length.
   */
  public ComposeSource(String bucketName, String objectName, Long offset, Long length)
    throws InvalidArgumentException {
    this(bucketName, objectName, offset, length, null, null, null);
  }

  /**
   * Create new ComposeSource for given bucket, object, offset, length and headerMap.
   */
  public ComposeSource(String bucketName, String objectName, Long offset, Long length,
      Map<String, String> headerMap) throws InvalidArgumentException {
    this(bucketName, objectName, offset, length, headerMap, null, null);
  }

  /**
   * Create new ComposeSource for given bucket, object, offset, length, headerMap and CopyConditions.
   */
  public ComposeSource(String bucketName, String objectName, Long offset, Long length,
      Map<String, String> headerMap, CopyConditions copyConditions) throws InvalidArgumentException {
    this(bucketName, objectName, offset, length, headerMap, copyConditions, null);
  }

  /**
   * Creates new ComposeSource for given bucket, object, offset, length, headerMap, CopyConditions
   * and server side encryption.
   *
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   */
  public ComposeSource(String bucketName, String objectName, Long offset, Long length, Map<String, String> headerMap,
      CopyConditions copyConditions, ServerSideEncryption sse) throws InvalidArgumentException {
    if (bucketName == null) {
      throw new InvalidArgumentException("Source bucket name cannot be empty");
    }

    if (objectName == null) {
      throw new InvalidArgumentException("Source object name cannot be empty");
    }

    if (offset != null && offset < 0) {
      throw new InvalidArgumentException("Offset cannot be negative");
    }

    if (length != null && length < 0) {
      throw new InvalidArgumentException("Length cannot be negative");
    }

    if (length != null && offset == null) {
      offset = 0L;
    }

    this.bucketName = bucketName;
    this.objectName = objectName;
    this.offset = offset;
    this.length = length;
    this.headerMap = headerMap;
    this.copyConditions = copyConditions;
    this.sse = sse;
  }

  /**
   * Constructs header  .
   *
   */
  public void buildHeaders(long objectSize, String etag) throws InvalidArgumentException {
    if (offset != null && offset >= objectSize) {
      throw new InvalidArgumentException("source " + bucketName + "/" + objectName + ": offset " + offset
        + " is beyond object size " + objectSize);
    }

    if (length != null) {
      if (length > objectSize) {
        throw new InvalidArgumentException("source " + bucketName + "/" + objectName + ": length " + length
          + " is beyond object size " + objectSize);
      }

      if (offset + length > objectSize) {
        throw new InvalidArgumentException("source " + bucketName + "/" + objectName + ": compose size "
          + (offset + length) + " is beyond object size " + objectSize);
      }
    }

    Map<String, String> headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    headers.put("x-amz-copy-source", S3Escaper.encodePath(bucketName + "/" + objectName));
    headers.put("x-amz-copy-source-if-match", etag);

    if (headerMap != null) {
      headers.putAll(headerMap);
    }

    if (copyConditions != null) {
      headers.putAll(copyConditions.getConditions());
    }

    if (sse != null) {
      headers.putAll(sse.copySourceHeaders());
    }

    this.objectSize = objectSize;
    this.headers = headers;
  }

  public String bucketName() {
    return bucketName;
  }

  public String objectName() {
    return objectName;
  }

  public Long offset() {
    return offset;
  }

  public Long length() {
    return length;
  }

  public CopyConditions copyConditions() {
    return copyConditions;
  }

  public ServerSideEncryption sse() {
    return sse;
  }

  /**
   * Returns header.
   *
   */
  public Map<String, String> headers() {
    Map<String, String> headers = null;

    if (this.headers != null) {
      headers = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      headers.putAll(this.headers);
    }

    return headers;
  }

  public long objectSize() {
    return objectSize;
  }
}
