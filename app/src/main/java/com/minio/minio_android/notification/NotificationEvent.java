/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage,
 * (C) 2018 MinIO, Inc.
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

package com.minio.minio_android.notification;

import java.util.Map;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings("UUF_UNUSED_PUBLIC_OR_PROTECTED_FIELD")
@JsonIgnoreProperties(ignoreUnknown = true)
public class NotificationEvent {
  public String eventVersion;
  public String eventSource;
  public String awsRegion;
  public String eventTime;
  public String eventName;
  public Identity userIdentity;
  public Map<String, String> requestParameters;
  public Map<String, String> responseElements;
  public EventMeta s3;
  public SourceInfo source;
}

