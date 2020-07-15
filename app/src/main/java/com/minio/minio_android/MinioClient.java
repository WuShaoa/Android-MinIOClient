/*
 * MinIO Java SDK for Amazon S3 Compatible Cloud Storage,
 * (C) 2015, 2016, 2017, 2018, 2019 MinIO, Inc.
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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.io.ByteStreams;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import com.minio.minio_android.errors.BucketPolicyTooLargeException;
import com.minio.minio_android.errors.ErrorResponseException;
import com.minio.minio_android.errors.InsufficientDataException;
import com.minio.minio_android.errors.InternalException;
import com.minio.minio_android.errors.InvalidArgumentException;
import com.minio.minio_android.errors.InvalidBucketNameException;
import com.minio.minio_android.errors.InvalidEndpointException;
import com.minio.minio_android.errors.InvalidExpiresRangeException;
import com.minio.minio_android.errors.InvalidObjectPrefixException;
import com.minio.minio_android.errors.InvalidPortException;
import com.minio.minio_android.errors.InvalidResponseException;
import com.minio.minio_android.errors.NoResponseException;
import com.minio.minio_android.errors.RegionConflictException;
import com.minio.minio_android.http.HeaderParser;
import com.minio.minio_android.http.Method;
import com.minio.minio_android.http.Scheme;
import com.minio.minio_android.messages.Bucket;
import com.minio.minio_android.messages.CompleteMultipartUpload;
import com.minio.minio_android.messages.CopyObjectResult;
import com.minio.minio_android.messages.CopyPartResult;
import com.minio.minio_android.messages.CreateBucketConfiguration;
import com.minio.minio_android.messages.DeleteError;
import com.minio.minio_android.messages.DeleteObject;
import com.minio.minio_android.messages.DeleteRequest;
import com.minio.minio_android.messages.DeleteResult;
import com.minio.minio_android.messages.ErrorResponse;
import com.minio.minio_android.messages.InitiateMultipartUploadResult;
import com.minio.minio_android.messages.Item;
import com.minio.minio_android.messages.ListAllMyBucketsResult;
import com.minio.minio_android.messages.ListBucketResult;
import com.minio.minio_android.messages.ListBucketResultV1;
import com.minio.minio_android.messages.ListMultipartUploadsResult;
import com.minio.minio_android.messages.ListPartsResult;
import com.minio.minio_android.messages.ObjectLockConfiguration;
import com.minio.minio_android.messages.Part;
import com.minio.minio_android.messages.Prefix;
import com.minio.minio_android.messages.Upload;
import com.minio.minio_android.messages.NotificationConfiguration;
import com.minio.minio_android.org.apache.commons.validator.routines.InetAddressValidator;

import com.minio.minio_android.notification.NotificationInfo;

import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;
import okhttp3.Protocol;

import org.joda.time.DateTime;
import org.xmlpull.v1.XmlPullParser;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.io.StringReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.KeyManagementException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/**
 * <p>
 * This class implements a simple cloud storage client. This client consists
 * of a useful subset of S3 compatible functionality.
 * </p>
 * <h2>Service</h2>
 * <ul>
 * <li>Creating a bucket</li>
 * <li>Listing buckets</li>
 * </ul>
 * <h2>Bucket</h2>
 * <ul>
 * <li> Creating an object, including automatic multipart for large objects.</li>
 * <li> Listing objects in a bucket</li>
 * <li> Listing active multipart uploads</li>
 * </ul>
 * <h2>Object</h2>
 * <ul>
 * <li>Removing an active multipart upload for a specific object and uploadId</li>
 * <li>Read object metadata</li>
 * <li>Reading an object</li>
 * <li>Reading a range of bytes of an object</li>
 * <li>Deleting an object</li>
 * </ul>
 * <p>
 * Optionally, users can also provide access/secret keys. If keys are provided, all requests by the
 * client will be signed using AWS Signature Version 4.
 * </p>
 * For examples on using this library, please see
 * <a href="https://github.com/minio/minio-java/tree/master/src/test/java/io/minio/examples"></a>.
 */
@SuppressWarnings({"SameParameterValue", "WeakerAccess"})
public class MinioClient {
  private static final Logger LOGGER = Logger.getLogger(MinioClient.class.getName());
  // default network I/O timeout is 15 minutes
  private static final long DEFAULT_CONNECTION_TIMEOUT = 15 * 60;
  // maximum allowed object size is 5TiB
  private static final long MAX_OBJECT_SIZE = 5L * 1024 * 1024 * 1024 * 1024;
  // maxPartSize - maximum part size 5GiB for a single multipart upload operation
  private static final long MAX_PART_SIZE = 5L * 1024 * 1024 * 1024;
  private static final int MAX_MULTIPART_COUNT = 10000;
  // minimum allowed multipart size is 5MiB
  private static final int MIN_MULTIPART_SIZE = 5 * 1024 * 1024;
  // maximum allowed bucket policy size is 12KiB
  private static final int MAX_BUCKET_POLICY_SIZE = 12 * 1024;
  // default expiration for a presigned URL is 7 days in seconds
  private static final int DEFAULT_EXPIRY_TIME = 7 * 24 * 3600;
  private static final String DEFAULT_USER_AGENT = "MinIO (" + System.getProperty("os.arch") + "; "
      + System.getProperty("os.arch") + ") minio-java/" + MinioProperties.INSTANCE.getVersion();
  private static final String NULL_STRING = "(null)";
  private static final String S3_AMAZONAWS_COM = "s3.amazonaws.com";
  private static final String END_HTTP = "----------END-HTTP----------";
  private static final String US_EAST_1 = "us-east-1";
  private static final String UPLOAD_ID = "uploadId";

  private static XmlPullParserFactory xmlPullParserFactory = null;

  private static final Set<String> amzHeaders = new HashSet<>();

  static {
    amzHeaders.add("server-side-encryption");
    amzHeaders.add("server-side-encryption-aws-kms-key-id");
    amzHeaders.add("server-side-encryption-context");
    amzHeaders.add("server-side-encryption-customer-algorithm");
    amzHeaders.add("server-side-encryption-customer-key");
    amzHeaders.add("server-side-encryption-customer-key-md5");
    amzHeaders.add("website-redirect-location");
    amzHeaders.add("storage-class");
  }

  private static final Set<String> standardHeaders = new HashSet<>();

  static {
    standardHeaders.add("content-type");
    standardHeaders.add("cache-control");
    standardHeaders.add("content-encoding");
    standardHeaders.add("content-disposition");
    standardHeaders.add("content-language");
    standardHeaders.add("expires");
    standardHeaders.add("range");
  }

  static {
    try {
      xmlPullParserFactory = XmlPullParserFactory.newInstance();
      xmlPullParserFactory.setNamespaceAware(true);
    } catch (XmlPullParserException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private PrintWriter traceStream;

  // the current client instance's base URL.
  private HttpUrl baseUrl;
  // access key to sign all requests with
  private String accessKey;
  // Secret key to sign all requests with
  private String secretKey;
  // Region to sign all requests with
  private String region;

  private String userAgent = DEFAULT_USER_AGENT;

  private OkHttpClient httpClient;


  /**
   * Creates MinIO client object with given endpoint using anonymous access.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code MinioClient minioClient = new MinioClient("https://play.min.io"); }</pre>
   int. Endpoint is an URL, domain name, IPv4 or IPv6 address.<pre>
   *              Valid endpoints:
   *              * https://s3.amazonaws.com
   *              * https://s3.amazonaws.com/
   *              * https://play.min.io
   *              * http://play.min.io:9010/
   *              * localhost
   *              * localhost.localdomain
   *              * play.min.io
   *              * 127.0.0.1
   *              * 192.168.1.60   *
   * @param endpoint  Request endpo
   *              * ::1</pre>
   *
   * @see #MinioClient(URL url)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey, String region)
//   * @see #MinioClient(URL url, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, String region, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, String region, boolean secure,
   *          OkHttpClient httpClient)
   */
  public MinioClient(String endpoint) throws InvalidEndpointException, InvalidPortException {
    this(endpoint, 0, null, null);
  }


  /**
   * Creates MinIO client object with given URL object using anonymous access.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code MinioClient minioClient = new MinioClient(new URL("https://play.min.io")); }</pre>
   *
   * @param url Endpoint URL object.
   *
//   * @see #MinioClient(String endpoint)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey, String region)
//   * @see #MinioClient(URL url, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, String region, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, String region, boolean secure,
   *          OkHttpClient httpClient)
   */
  public MinioClient(URL url) throws InvalidEndpointException, InvalidPortException {
    this(url.toString(), 0, null, null);
  }

  /**
   * Creates MinIO client object with given HttpUrl object using anonymous access.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code MinioClient minioClient = new MinioClient(new HttpUrl.parse("https://play.min.io")); }</pre>
   *
   * @param url Endpoint HttpUrl object.
   *
//   * @see #MinioClient(String endpoint)
//   * @see #MinioClient(URL url)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey, String region)
//   * @see #MinioClient(URL url, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, String region, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, String region, boolean secure,
   *          OkHttpClient httpClient)
   */
  public MinioClient(HttpUrl url) throws InvalidEndpointException, InvalidPortException {
    this(url.toString(), 0, null, null);
  }

  /**
   * Creates MinIO client object with given endpoint, access key and secret key.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code MinioClient minioClient = new MinioClient("https://play.min.io",
   *                            "YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY"); }</pre>
   * @param endpoint  Request endpoint. Endpoint is an URL, domain name, IPv4 or IPv6 address.<pre>
   *              Valid endpoints:
   *              * https://s3.amazonaws.com
   *              * https://s3.amazonaws.com/
   *              * https://play.min.io
   *              * http://play.min.io:9010/
   *              * localhost
   *              * localhost.localdomain
   *              * play.min.io
   *              * 127.0.0.1
   *              * 192.168.1.60
   *              * ::1</pre>
   * @param accessKey Access key to access service in endpoint.
   * @param secretKey Secret key to access service in endpoint.
//   *
//   * @see #MinioClient(String endpoint)
//   * @see #MinioClient(URL url)
//   * @see #MinioClient(URL url, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey, String region)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, String region, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, String region, boolean secure,
   *          OkHttpClient httpClient)
   */
  public MinioClient(String endpoint, String accessKey, String secretKey)
    throws InvalidEndpointException, InvalidPortException {
    this(endpoint, 0, accessKey, secretKey);
  }

  /**
   * Creates MinIO client object with given endpoint, access key, secret key and region name
   *
   * </p><b>Example:</b><br>
   * <pre>{@code MinioClient minioClient = new MinioClient("https://play.min.io",
   *                            "YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", "us-east-1"); }</pre>
   * @param endpoint  Request endpoint. Endpoint is an URL, domain name, IPv4 or IPv6 address.<pre>
   *              Valid endpoints:
   *              * https://s3.amazonaws.com
   *              * https://s3.amazonaws.com/
   *              * https://play.min.io
   *              * http://play.min.io:9010/
   *              * localhost
   *              * localhost.localdomain
   *              * play.min.io
   *              * 127.0.0.1
   *              * 192.168.1.60
   *              * ::1</pre>
   * @param accessKey Access key to access service in endpoint.
   * @param secretKey Secret key to access service in endpoint.
   * @param region Region name to access service in endpoint.
   *
//   * @see #MinioClient(String endpoint)
//   * @see #MinioClient(URL url)
//   * @see #MinioClient(URL url, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey, String region)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, String region, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, String region, boolean secure,
   *          OkHttpClient httpClient)
   */
  public MinioClient(String endpoint, String accessKey, String secretKey, String region)
    throws InvalidEndpointException, InvalidPortException {
    this(endpoint, 0, accessKey, secretKey, region, !(endpoint != null && endpoint.startsWith("http://")));
  }

  /**
   * Creates MinIO client object with given URL object, access key and secret key.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code MinioClient minioClient = new MinioClient(new URL("https://play.min.io"),
   *                            "YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY"); }</pre>
   *
   * @param url Endpoint URL object.
   * @param accessKey Access key to access service in endpoint.
   * @param secretKey Secret key to access service in endpoint.
   *
//   * @see #MinioClient(String endpoint)
//   * @see #MinioClient(URL url)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey, String region)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, String region, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, String region, boolean secure,
   *          OkHttpClient httpClient)
   */
  public MinioClient(URL url, String accessKey, String secretKey)
    throws InvalidEndpointException, InvalidPortException {
    this(url.toString(), 0, accessKey, secretKey);
  }

  /**
   * Creates MinIO client object with given URL object, access key and secret key.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code MinioClient minioClient = new MinioClient(HttpUrl.parse("https://play.min.io"),
   *                            "YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY"); }</pre>
   *
   * @param url Endpoint HttpUrl object.
   * @param accessKey Access key to access service in endpoint.
   * @param secretKey Secret key to access service in endpoint.
   *
//   * @see #MinioClient(String endpoint)
//   * @see #MinioClient(URL url)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey, String region)
//   * @see #MinioClient(URL url, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, String region, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, String region, boolean secure,
   *          OkHttpClient httpClient)
   */
  public MinioClient(HttpUrl url, String accessKey, String secretKey)
      throws InvalidEndpointException, InvalidPortException {
    this(url.toString(), 0, accessKey, secretKey);
  }

  /**
   * Creates MinIO client object with given endpoint, port, access key and secret key using secure (HTTPS) connection.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code MinioClient minioClient =
   *                  new MinioClient("play.min.io", 9000, "YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY");
   * }</pre>
   *
   * @param endpoint  Request endpoint. Endpoint is an URL, domain name, IPv4 or IPv6 address.<pre>
   *              Valid endpoints:
   *              * https://s3.amazonaws.com
   *              * https://s3.amazonaws.com/
   *              * https://play.min.io
   *              * http://play.min.io:9010/
   *              * localhost
   *              * localhost.localdomain
   *              * play.min.io
   *              * 127.0.0.1
   *              * 192.168.1.60
   *              * ::1</pre>
   *
   * @param port      Valid port.  It should be in between 1 and 65535.  Unused if endpoint is an URL.
   * @param accessKey Access key to access service in endpoint.
   * @param secretKey Secret key to access service in endpoint.
   *
//   * @see #MinioClient(String endpoint)
//   * @see #MinioClient(URL url)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey, String region)
//   * @see #MinioClient(URL url, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, String region, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, String region, boolean secure,
   *          OkHttpClient httpClient)
   */
  public MinioClient(String endpoint, int port, String accessKey, String secretKey)
    throws InvalidEndpointException, InvalidPortException {
    this(endpoint, port, accessKey, secretKey, !(endpoint != null && endpoint.startsWith("http://")));
  }

  /**
   * Creates MinIO client object with given endpoint, access key and secret key using secure (HTTPS) connection.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code MinioClient minioClient =
   *                      new MinioClient("play.min.io", "YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", true);
   * }</pre>
   *
   * @param endpoint  Request endpoint. Endpoint is an URL, domain name, IPv4 or IPv6 address.<pre>
   *              Valid endpoints:
   *              * https://s3.amazonaws.com
   *              * https://s3.amazonaws.com/
   *              * https://play.min.io
   *              * http://play.min.io:9010/
   *              * localhost
   *              * localhost.localdomain
   *              * play.min.io
   *              * 127.0.0.1
   *              * 192.168.1.60
   *              * ::1</pre>
   *
   * @param accessKey Access key to access service in endpoint.
   * @param secretKey Secret key to access service in endpoint.
   * @param secure If true, access endpoint using HTTPS else access it using HTTP.
   *
//   * @see #MinioClient(String endpoint)
//   * @see #MinioClient(URL url)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey, String region)
//   * @see #MinioClient(URL url, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, String region, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, String region, boolean secure,
   *          OkHttpClient httpClient)
   */
  public MinioClient(String endpoint, String accessKey, String secretKey, boolean secure)
    throws InvalidEndpointException, InvalidPortException {
    this(endpoint, 0, accessKey, secretKey, secure);
  }

  /**
   * Creates MinIO client object using given endpoint, port, access key, secret key and secure option.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code MinioClient minioClient =
   *          new MinioClient("play.min.io", 9000, "YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", false);
   * }</pre>
   *
   * @param endpoint  Request endpoint. Endpoint is an URL, domain name, IPv4 or IPv6 address.<pre>
   *              Valid endpoints:
   *              * https://s3.amazonaws.com
   *              * https://s3.amazonaws.com/
   *              * https://play.min.io
   *              * http://play.min.io:9010/
   *              * localhost
   *              * localhost.localdomain
   *              * play.min.io
   *              * 127.0.0.1
   *              * 192.168.1.60
   *              * ::1</pre>
   *
   * @param port      Valid port.  It should be in between 1 and 65535.  Unused if endpoint is an URL.
   * @param accessKey Access key to access service in endpoint.
   * @param secretKey Secret key to access service in endpoint.
   * @param secure    If true, access endpoint using HTTPS else access it using HTTP.
   *
//   * @see #MinioClient(String endpoint)
//   * @see #MinioClient(URL url)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey, String region)
//   * @see #MinioClient(URL url, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, String region, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, String region, boolean secure,
   *          OkHttpClient httpClient)
   */
  public MinioClient(String endpoint, int port, String accessKey, String secretKey, boolean secure)
    throws InvalidEndpointException, InvalidPortException {
    this(endpoint, port, accessKey, secretKey, null, secure);
  }

  /**
   * Creates MinIO client object using given endpoint, port, access key, secret key, region and secure option.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code MinioClient minioClient =
   *          new MinioClient("play.min.io", 9000, "YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", "us-east-1", false);
   * }</pre>
   *
   * @param endpoint  Request endpoint. Endpoint is an URL, domain name, IPv4 or IPv6 address.<pre>
   *              Valid endpoints:
   *              * https://s3.amazonaws.com
   *              * https://s3.amazonaws.com/
   *              * https://play.min.io
   *              * http://play.min.io:9010/
   *              * localhost
   *              * localhost.localdomain
   *              * play.min.io
   *              * 127.0.0.1
   *              * 192.168.1.60
   *              * ::1</pre>
   *
   * @param port      Valid port.  It should be in between 1 and 65535.  Unused if endpoint is an URL.
   * @param accessKey Access key to access service in endpoint.
   * @param secretKey Secret key to access service in endpoint.
   * @param region    Region name to access service in endpoint.
   * @param secure    If true, access endpoint using HTTPS else access it using HTTP.
   *
//   * @see #MinioClient(String endpoint)
//   * @see #MinioClient(URL url)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey, String region)
//   * @see #MinioClient(URL url, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, String region, boolean secure,
   *          OkHttpClient httpClient)
   */
  public MinioClient(String endpoint, int port, String accessKey, String secretKey, String region, boolean secure)
    throws InvalidEndpointException, InvalidPortException {
    this(endpoint, port, accessKey, secretKey, region, secure, null);
  }


  /**
   * Creates MinIO client object using given endpoint, port, access key, secret key, region and secure option.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code MinioClient minioClient =
   *          new MinioClient("play.min.io", 9000, "YOUR-ACCESSKEYID", "YOUR-SECRETACCESSKEY", "us-east-1", false,
   *          customHttpClient);
   * }</pre>
   *
   * @param endpoint  Request endpoint. Endpoint is an URL, domain name, IPv4 or IPv6 address.<pre>
   *              Valid endpoints:
   *              * https://s3.amazonaws.com
   *              * https://s3.amazonaws.com/
   *              * https://play.min.io
   *              * http://play.min.io:9010/
   *              * localhost
   *              * localhost.localdomain
   *              * play.min.io
   *              * 127.0.0.1
   *              * 192.168.1.60
   *              * ::1</pre>
   *
   * @param port       Valid port.  It should be in between 1 and 65535.  Unused if endpoint is an URL.
   * @param accessKey  Access key to access service in endpoint.
   * @param secretKey  Secret key to access service in endpoint.
   * @param region     Region name to access service in endpoint.
   * @param secure     If true, access endpoint using HTTPS else access it using HTTP.
   * @param httpClient Customized HTTP client object.
   *
//   * @see #MinioClient(String endpoint)
//   * @see #MinioClient(URL url)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey, String region)
//   * @see #MinioClient(URL url, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey)
//   * @see #MinioClient(String endpoint, String accessKey, String secretKey, boolean secure)
//   * @see #MinioClient(String endpoint, int port, String accessKey, String secretKey, String region, boolean secure)
   */
  public MinioClient(String endpoint, int port, String accessKey, String secretKey, String region, boolean secure,
                     OkHttpClient httpClient)
    throws InvalidEndpointException, InvalidPortException {
    if (endpoint == null) {
      throw new InvalidEndpointException(NULL_STRING, "null endpoint");
    }

    if (port < 0 || port > 65535) {
      throw new InvalidPortException(port, "port must be in range of 1 to 65535");
    }

    if (httpClient != null) {
      this.httpClient = httpClient;
    } else {
      List<Protocol> protocol = new LinkedList<>();
      protocol.add(Protocol.HTTP_1_1);
      this.httpClient = new OkHttpClient();
      this.httpClient = this.httpClient.newBuilder()
        .connectTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.SECONDS)
        .writeTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.SECONDS)
        .readTimeout(DEFAULT_CONNECTION_TIMEOUT, TimeUnit.SECONDS)
        .protocols(protocol)
        .build();
    }

    HttpUrl url = HttpUrl.parse(endpoint);
    if (url != null) {
      if (!"/".equals(url.encodedPath())) {
        throw new InvalidEndpointException(endpoint, "no path allowed in endpoint");
      }

      HttpUrl.Builder urlBuilder = url.newBuilder();
      Scheme scheme = Scheme.HTTP;
      if (secure) {
        scheme = Scheme.HTTPS;
      }

      urlBuilder.scheme(scheme.toString());

      if (port > 0) {
        urlBuilder.port(port);
      }

      this.baseUrl = urlBuilder.build();
      this.accessKey = accessKey;
      this.secretKey = secretKey;
      this.region = region;

      return;
    }

    // endpoint may be a valid hostname, IPv4 or IPv6 address
    if (!this.isValidEndpoint(endpoint)) {
      throw new InvalidEndpointException(endpoint, "invalid host");
    }

    Scheme scheme = Scheme.HTTP;
    if (secure) {
      scheme = Scheme.HTTPS;
    }

    if (port == 0) {
      this.baseUrl = new HttpUrl.Builder()
          .scheme(scheme.toString())
          .host(endpoint)
          .build();
    } else {
      this.baseUrl = new HttpUrl.Builder()
          .scheme(scheme.toString())
          .host(endpoint)
          .port(port)
          .build();
    }
    this.accessKey = accessKey;
    this.secretKey = secretKey;
    this.region = region;
  }

  /**
   * Returns true if given endpoint is valid else false.
   */
  private boolean isValidEndpoint(String endpoint) {
    if (InetAddressValidator.getInstance().isValid(endpoint)) {
      return true;
    }

    // endpoint may be a hostname
    // refer https://en.wikipedia.org/wiki/Hostname#Restrictions_on_valid_host_names
    // why checks are done like below
    if (endpoint.length() < 1 || endpoint.length() > 253) {
      return false;
    }

    for (String label : endpoint.split("\\.")) {
      if (label.length() < 1 || label.length() > 63) {
        return false;
      }

      if (!(label.matches("^[a-zA-Z0-9]([a-zA-Z0-9-]*[a-zA-Z0-9])?$"))) {
        return false;
      }
    }

    return true;
  }


  /**
   * Validates if given bucket name is DNS compatible.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   */
  private void checkBucketName(String name) throws InvalidBucketNameException {
    if (name == null) {
      throw new InvalidBucketNameException(NULL_STRING, "null bucket name");
    }

    // Bucket names cannot be no less than 3 and no more than 63 characters long.
    if (name.length() < 3 || name.length() > 63) {
      String msg = "bucket name must be at least 3 and no more than 63 characters long";
      throw new InvalidBucketNameException(name, msg);
    }
    // Successive periods in bucket names are not allowed.
    if (name.matches("\\.\\.")) {
      String msg = "bucket name cannot contain successive periods. For more information refer "
          + "http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html";
      throw new InvalidBucketNameException(name, msg);
    }
    // Bucket names should be dns compatible.
    if (!name.matches("^[a-z0-9][a-z0-9\\.\\-]+[a-z0-9]$")) {
      String msg = "bucket name does not follow Amazon S3 standards. For more information refer "
          + "http://docs.aws.amazon.com/AmazonS3/latest/dev/BucketRestrictions.html";
      throw new InvalidBucketNameException(name, msg);
    }
  }


  /**
   * Sets HTTP connect, write and read timeouts.  A value of 0 means no timeout, otherwise values must be between 1 and
   * Integer.MAX_VALUE when converted to milliseconds.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code minioClient.setTimeout(TimeUnit.SECONDS.toMillis(10), TimeUnit.SECONDS.toMillis(10),
   *                            TimeUnit.SECONDS.toMillis(30)); }</pre>
   *
   * @param connectTimeout    HTTP connect timeout in milliseconds.
   * @param writeTimeout      HTTP write timeout in milliseconds.
   * @param readTimeout       HTTP read timeout in milliseconds.
   */
  public void setTimeout(long connectTimeout, long writeTimeout, long readTimeout) {
    this.httpClient = this.httpClient.newBuilder()
      .connectTimeout(connectTimeout, TimeUnit.MILLISECONDS)
      .writeTimeout(writeTimeout, TimeUnit.MILLISECONDS)
      .readTimeout(readTimeout, TimeUnit.MILLISECONDS)
      .build();
  }


  /**
   * Ignores check on server certificate for HTTPS connection.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code minioClient.ignoreCertCheck(); }</pre>
   *
   */
  @SuppressFBWarnings(value = "SIC", justification = "Should not be used in production anyways.")
  public void ignoreCertCheck() throws NoSuchAlgorithmException, KeyManagementException {
    final TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
          return new X509Certificate[]{};
        }
      }
    };

    final SSLContext sslContext = SSLContext.getInstance("SSL");
    sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
    final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

    this.httpClient = this.httpClient.newBuilder()
      .sslSocketFactory(sslSocketFactory, (X509TrustManager)trustAllCerts[0])
      .hostnameVerifier(new HostnameVerifier() {
          @Override
          public boolean verify(String hostname, SSLSession session) {
            return true;
          }
        })
      .build();
  }


  /**
   * Creates Request object for given request parameters.
   *
   * @param method         HTTP method.
   * @param bucketName     Bucket name.
   * @param objectName     Object name in the bucket.
   * @param region         Amazon S3 region of the bucket.
   * @param headerMap      Map of HTTP headers for the request.
   * @param queryParamMap  Map of HTTP query parameters of the request.
   * @param contentType    Content type of the request body.
   * @param body           HTTP request body.
   * @param length         Length of HTTP request body.
   */
  private Request createRequest(Method method, String bucketName, String objectName,
                                String region, Multimap<String,String> headerMap,
                                Multimap<String,String> queryParamMap, final String contentType,
                                Object body, int length)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InvalidKeyException, InsufficientDataException,
           IOException, InternalException {
    if (bucketName == null && objectName != null) {
      throw new InvalidBucketNameException(NULL_STRING, "null bucket name for object '" + objectName + "'");
    }

    HttpUrl.Builder urlBuilder = this.baseUrl.newBuilder();
    if (bucketName != null) {
      checkBucketName(bucketName);

      String host = this.baseUrl.host();
      if (host.equals(S3_AMAZONAWS_COM)) {
        // special case: handle s3.amazonaws.com separately
        if (region != null) {
          host = AwsS3Endpoints.INSTANCE.endpoint(region);
        }

        boolean usePathStyle = false;
        if (method == Method.PUT && objectName == null && queryParamMap == null) {
          // use path style for make bucket to workaround "AuthorizationHeaderMalformed" error from s3.amazonaws.com
          usePathStyle = true;
        } else if (queryParamMap != null && queryParamMap.containsKey("location")) {
          // use path style for location query
          usePathStyle = true;
        } else if (bucketName.contains(".") && this.baseUrl.isHttps()) {
          // use path style where '.' in bucketName causes SSL certificate validation error
          usePathStyle = true;
        }

        if (usePathStyle) {
          urlBuilder.host(host);
          urlBuilder.addEncodedPathSegment(S3Escaper.encode(bucketName));
        } else {
          urlBuilder.host(bucketName + "." + host);
        }
      } else {
        urlBuilder.addEncodedPathSegment(S3Escaper.encode(bucketName));
      }
    }

    if (objectName != null) {
      // Limitation: OkHttp does not allow to add '.' and '..' as path segment.
      urlBuilder.addEncodedPathSegments(S3Escaper.encodePath(objectName));
    }

    if (queryParamMap != null) {
      for (Map.Entry<String,String> entry : queryParamMap.entries()) {
        urlBuilder.addEncodedQueryParameter(S3Escaper.encode(entry.getKey()), S3Escaper.encode(entry.getValue()));
      }
    }

    HttpUrl url = urlBuilder.build();

    Request.Builder requestBuilder = new Request.Builder();
    requestBuilder.url(url);
    if (headerMap != null) {
      for (Map.Entry<String,String> entry : headerMap.entries()) {
        requestBuilder.header(entry.getKey(), entry.getValue());
      }
    }

    String sha256Hash = null;
    String md5Hash = null;
    boolean chunkedUpload = false;
    if (this.accessKey != null && this.secretKey != null) {
      // Handle putobject specially to use chunked upload.
      if (method == Method.PUT && objectName != null && body != null && body instanceof InputStream && length > 0) {
        sha256Hash = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

        String contentEncoding = "aws-chunked";
        if (headerMap != null) {
          contentEncoding = Stream.concat(Stream.of("aws-chunked"),
              headerMap.get("Content-Encoding").stream()).distinct().filter(encoding -> !encoding.isEmpty())
              .collect(Collectors.joining(","));
        }
        requestBuilder.header("Content-Encoding", contentEncoding);

        requestBuilder.header("x-amz-decoded-content-length", Integer.toString(length));
        chunkedUpload = true;
      } else if (url.isHttps()) {
        // Fix issue #415: No need to compute sha256 if endpoint scheme is HTTPS.
        sha256Hash = "UNSIGNED-PAYLOAD";
        if (body != null) {
          md5Hash = Digest.md5Hash(body, length);
        }
      } else {
        Object data = body;
        int len = length;
        if (data == null) {
          data = new byte[0];
          len = 0;
        }

        if (method == Method.POST && queryParamMap != null && queryParamMap.containsKey("delete")) {
          // Fix issue #579: Treat 'Delete Multiple Objects' specially which requires MD5 hash.
          String[] hashes = Digest.sha256Md5Hashes(data, len);
          sha256Hash = hashes[0];
          md5Hash = hashes[1];
        } else if (method == Method.PUT && queryParamMap != null && queryParamMap.containsKey("lifecycle")) {
          String[] hashes = Digest.sha256Md5Hashes(data, len);
          sha256Hash = hashes[0];
          md5Hash = hashes[1];
        } else {
          // Fix issue #567: Compute SHA256 hash only.
          sha256Hash = Digest.sha256Hash(data, len);
        }
      }
    } else {
      // Fix issue #567: Compute MD5 hash only for anonymous access.
      if (body != null) {
        md5Hash = Digest.md5Hash(body, length);
      }
    }

    if (md5Hash != null) {
      requestBuilder.header("Content-MD5", md5Hash);
    }
    if (this.shouldOmitPortInHostHeader(url)) {
      requestBuilder.header("Host", url.host());
    } else {
      requestBuilder.header("Host", url.host() + ":" + url.port());
    }
    requestBuilder.header("User-Agent", this.userAgent);
    if (sha256Hash != null) {
      requestBuilder.header("x-amz-content-sha256", sha256Hash);
    }
    DateTime date = new DateTime();
    requestBuilder.header("x-amz-date", date.toString(DateFormat.AMZ_DATE_FORMAT));

    if (chunkedUpload) {
      // Add empty request body for calculating seed signature.
      // The actual request body is properly set below.
      requestBuilder.method(method.toString(), RequestBody.create(null, ""));
      Request request = requestBuilder.build();
      String seedSignature = Signer.getChunkSeedSignature(request, region, secretKey);
      requestBuilder = request.newBuilder();

      ChunkedInputStream cis = new ChunkedInputStream((InputStream) body, length, date, region, this.secretKey,
                                                      seedSignature);
      body = cis;
      length = cis.length();
    }

    RequestBody requestBody = null;
    if (body != null) {
      requestBody = new HttpRequestBody(contentType, body, length);
    }

    requestBuilder.method(method.toString(), requestBody);
    return requestBuilder.build();
  }


  /**
   * Checks whether port should be omitted in Host header.
   *
   * <p>
   * HTTP Spec (rfc2616) defines that port should be omitted in Host header
   * when port and service matches (i.e HTTP -> 80, HTTPS -> 443)
   *
   * @param url Url object
   */
  private boolean shouldOmitPortInHostHeader(HttpUrl url) {
    return (url.scheme().equals("http") && url.port() == 80)
      || (url.scheme().equals("https") && url.port() == 443);
  }


  /**
   * Executes given request parameters.
   *
   * @param method         HTTP method.
   * @param region         Amazon S3 region of the bucket.
   * @param bucketName     Bucket name.
   * @param objectName     Object name in the bucket.
   * @param headerMap      Map of HTTP headers for the request.
   * @param queryParamMap  Map of HTTP query parameters of the request.
   * @param body           HTTP request body.
   * @param length         Length of HTTP request body.
   */
  private HttpResponse execute(Method method, String region, String bucketName, String objectName,
                               Map<String,String> headerMap, Map<String,String> queryParamMap,
                               Object body, int length)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException {

    if (headerMap != null) {
      headerMap = normalizeHeaders(headerMap);
    }

    Multimap<String, String> queryParamMultiMap = null;
    if (queryParamMap != null) {
      queryParamMultiMap = Multimaps.forMap(queryParamMap);
    }

    Multimap<String, String> headerMultiMap = null;
    if (headerMap != null) {
      headerMultiMap = Multimaps.forMap(headerMap);
    }

    return executeReq(method, region, bucketName, objectName, headerMultiMap, queryParamMultiMap, body, length);
  }

  private HttpResponse executeReq(Method method, String region, String bucketName, String objectName,
                               Multimap<String,String> headerMap, Multimap<String,String> queryParamMap,
                               Object body, int length)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException {
    String contentType = null;
    if (headerMap != null && headerMap.get("Content-Type") != null) {
      contentType = String.join(" ", headerMap.get("Content-Type"));
    }
    if (body != null && !(body instanceof InputStream || body instanceof RandomAccessFile || body instanceof byte[])) {
      byte[] bytes = body.toString().getBytes(StandardCharsets.UTF_8);
      body = bytes;
      length = bytes.length;
    }

    Request request = createRequest(method, bucketName, objectName, region,
                                    headerMap, queryParamMap,
                                    contentType, body, length);

    if (this.accessKey != null && this.secretKey != null) {
      request = Signer.signV4(request, region, accessKey, secretKey);
    }

    if (this.traceStream != null) {
      this.traceStream.println("---------START-HTTP---------");
      String encodedPath = request.url().encodedPath();
      String encodedQuery = request.url().encodedQuery();
      if (encodedQuery != null) {
        encodedPath += "?" + encodedQuery;
      }
      this.traceStream.println(request.method() + " " + encodedPath + " HTTP/1.1");
      String headers = request.headers().toString()
          .replaceAll("Signature=([0-9a-f]+)", "Signature=*REDACTED*")
          .replaceAll("Credential=([^/]+)", "Credential=*REDACTED*");
      this.traceStream.println(headers);
    }

    Response response = this.httpClient.newCall(request).execute();
    if (this.traceStream != null) {
      this.traceStream.println(response.protocol().toString().toUpperCase(Locale.US) + " " + response.code());
      this.traceStream.println(response.headers());
    }

    ResponseHeader header = new ResponseHeader();
    HeaderParser.set(response.headers(), header);

    if (response.isSuccessful()) {
      if (this.traceStream != null) {
        this.traceStream.println(END_HTTP);
      }
      return new HttpResponse(header, response);
    }

    ErrorResponse errorResponse = null;

    // HEAD returns no body, and fails on parseXml
    if (!method.equals(Method.HEAD)) {
      Scanner scanner = new Scanner(response.body().charStream());
      try {
        scanner.useDelimiter("\\A");
        String errorXml = "";

        // read entire body stream to string.
        if (scanner.hasNext()) {
          errorXml = scanner.next();
        }
        
        // Error in case of Non-XML response from server
        if (!("application/xml".equals(response.headers().get("content-type")))) {
          throw new InvalidResponseException();
        }
        errorResponse = new ErrorResponse(new StringReader(errorXml));
        if (this.traceStream != null) {
          this.traceStream.println(errorXml);
        }

      } finally {
        response.body().close();
        scanner.close();
      }
    }

    if (this.traceStream != null) {
      this.traceStream.println(END_HTTP);
    }

    if (errorResponse == null) {
      ErrorCode ec;
      switch (response.code()) {
        case 307:
          ec = ErrorCode.REDIRECT;
          break;
        case 400:
          ec = ErrorCode.INVALID_URI;
          break;
        case 404:
          if (objectName != null) {
            ec = ErrorCode.NO_SUCH_KEY;
          } else if (bucketName != null) {
            ec = ErrorCode.NO_SUCH_BUCKET;
          } else {
            ec = ErrorCode.RESOURCE_NOT_FOUND;
          }
          break;
        case 501:
        case 405:
          ec = ErrorCode.METHOD_NOT_ALLOWED;
          break;
        case 409:
          if (bucketName != null) {
            ec = ErrorCode.NO_SUCH_BUCKET;
          } else {
            ec = ErrorCode.RESOURCE_CONFLICT;
          }
          break;
        case 403:
          ec = ErrorCode.ACCESS_DENIED;
          break;
        default:
          throw new InternalException("unhandled HTTP code " + response.code() + ".  Please report this issue at "
                                      + "https://github.com/minio/minio-java/issues");
      }

      errorResponse = new ErrorResponse(ec, bucketName, objectName, request.url().encodedPath(),
                                        header.xamzRequestId(), header.xamzId2());
    }

    // invalidate region cache if needed
    if (errorResponse.errorCode() == ErrorCode.NO_SUCH_BUCKET) {
      BucketRegionCache.INSTANCE.remove(bucketName);
      // TODO: handle for other cases as well
      // observation: on HEAD of a bucket with wrong region gives 400 without body
    }

    throw new ErrorResponseException(errorResponse, response);
  }

  /**
   * Updates Region cache for given bucket.
   */
  private void updateRegionCache(String bucketName)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException {
    if (bucketName != null && this.accessKey != null && this.secretKey != null
          && !BucketRegionCache.INSTANCE.exists(bucketName)) {
      Map<String,String> queryParamMap = new HashMap<>();
      queryParamMap.put("location", null);

      HttpResponse response = execute(Method.GET, US_EAST_1, bucketName, null,
          null, queryParamMap, null, 0);

      // existing XmlEntity does not work, so fallback to regular parsing.
      XmlPullParser xpp = xmlPullParserFactory.newPullParser();
      String location = null;

      try (ResponseBody body = response.body()) {
        xpp.setInput(body.charStream());
        while (xpp.getEventType() != XmlPullParser.END_DOCUMENT) {
          if (xpp.getEventType() == XmlPullParser.START_TAG && "LocationConstraint".equals(xpp.getName())) {
            xpp.next();
            location = getText(xpp);
            break;
          }
          xpp.next();
        }
      }

      String region;
      if (location == null) {
        region = US_EAST_1;
      } else {
        // eu-west-1 can be sometimes 'EU'.
        if ("EU".equals(location)) {
          region = "eu-west-1";
        } else {
          region = location;
        }
      }

      // Add the new location.
      BucketRegionCache.INSTANCE.set(bucketName, region);
    }
  }

  /**
   * Computes region of a given bucket name. If set, this.region is considered. Otherwise,
   * resort to the server location API.
   */
  private String getRegion(String bucketName) throws InvalidBucketNameException, NoSuchAlgorithmException,
          InsufficientDataException, IOException, InvalidKeyException, NoResponseException, XmlPullParserException,
          ErrorResponseException, InternalException, InvalidResponseException {
    String region;
    if (this.region == null || "".equals(this.region)) {
      updateRegionCache(bucketName);
      region = BucketRegionCache.INSTANCE.region(bucketName);
    } else {
      region = this.region;
    }
    return region;
  }

  /**
   * Returns text of given XML element.
   *
   * @throws XmlPullParserException upon parsing response xml
   */
  private String getText(XmlPullParser xpp) throws XmlPullParserException {
    if (xpp.getEventType() == XmlPullParser.TEXT) {
      return xpp.getText();
    }
    return null;
  }

  private void checkReadRequestSse(ServerSideEncryption sse) throws InvalidArgumentException {
    if (sse == null) {
      return;
    }

    if (sse.type() != ServerSideEncryption.Type.SSE_C) {
      throw new InvalidArgumentException("only SSE_C is supported for all read requests.");
    }

    if (sse.type().requiresTls() && !this.baseUrl.isHttps()) {
      throw new InvalidArgumentException(sse.type().name()
                                         + "operations must be performed over a secure connection.");
    }
  }

  private void checkWriteRequestSse(ServerSideEncryption sse) throws InvalidArgumentException {
    if (sse == null) {
      return;
    }

    if (sse.type().requiresTls() && !this.baseUrl.isHttps()) {
      throw new InvalidArgumentException(sse.type().name()
                                         + " operations must be performed over a secure connection.");
    }
  }

  /**
   * Executes GET method for given request parameters.
   *
   * @param bucketName     Bucket name.
   * @param objectName     Object name in the bucket.
   * @param headerMap      Map of HTTP headers for the request.
   * @param queryParamMap  Map of HTTP query parameters of the request.
   */
  private HttpResponse executeGet(String bucketName, String objectName, Map<String,String> headerMap,
                                  Map<String,String> queryParamMap)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException {
    return execute(Method.GET, getRegion(bucketName), bucketName, objectName, headerMap, queryParamMap, null, 0);
  }


  /**
   * Executes HEAD method for given request parameters.
   *
   * @param bucketName     Bucket name.
   * @param objectName     Object name in the bucket.
   */
  private HttpResponse executeHead(String bucketName, String objectName)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException {
    HttpResponse response = execute(Method.HEAD, getRegion(bucketName), bucketName, objectName, null,
                                    null, null, 0);
    response.body().close();
    return response;
  }

  /**
   * Executes HEAD method for given request parameters.
   *
   * @param bucketName     Bucket name.
   * @param objectName     Object name in the bucket.
   * @param headerMap      Map of header parameters of the request.
   */
  private HttpResponse executeHead(String bucketName, String objectName, Map<String,String> headerMap)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException {

    HttpResponse response = execute(Method.HEAD, getRegion(bucketName), bucketName, objectName, headerMap,
                                    null, null, 0);
    response.body().close();
    return response;
  }

  /**
   * Executes DELETE method for given request parameters.
   *
   * @param bucketName     Bucket name.
   * @param objectName     Object name in the bucket.
   * @param queryParamMap  Map of HTTP query parameters of the request.
   */
  private HttpResponse executeDelete(String bucketName, String objectName, Map<String,String> queryParamMap)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException {
    HttpResponse response = execute(Method.DELETE, getRegion(bucketName), bucketName, objectName, null,
                                    queryParamMap, null, 0);
    response.body().close();
    return response;
  }


  /**
   * Executes POST method for given request parameters.
   *
   * @param bucketName     Bucket name.
   * @param objectName     Object name in the bucket.
   * @param headerMap      Map of HTTP headers for the request.
   * @param queryParamMap  Map of HTTP query parameters of the request.
   * @param data           HTTP request body data.
   */
  private HttpResponse executePost(String bucketName, String objectName, Map<String,String> headerMap,
                                   Map<String,String> queryParamMap, Object data)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException {
    return execute(Method.POST, getRegion(bucketName), bucketName, objectName, headerMap, queryParamMap, data, 0);
  }


  private Map<String, String> normalizeHeaders(Map<String,String> headerMap) {
    Map<String,String> normHeaderMap = new HashMap<String,String>();
    for (Map.Entry<String, String> entry : headerMap.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      String keyLowerCased = key.toLowerCase(Locale.US);
      if (amzHeaders.contains(keyLowerCased)) {
        key = "x-amz-" + key;
      } else if (!standardHeaders.contains(keyLowerCased)
                 && !keyLowerCased.startsWith("x-amz-")) {
        key = "x-amz-meta-" + key;
      }
      normHeaderMap.put(key, value);
    }
    return normHeaderMap;
  }

  /**
   * Executes PUT method for given request parameters.
   *
   * @param bucketName     Bucket name.
   * @param objectName     Object name in the bucket.
   * @param headerMap      Map of HTTP headers for the request.
   * @param queryParamMap  Map of HTTP query parameters of the request.
   * @param region         Amazon S3 region of the bucket.
   * @param data           HTTP request body data.
   * @param length         Length of HTTP request body data.
   */
  private HttpResponse executePut(String bucketName, String objectName, Map<String,String> headerMap,
                                  Map<String,String> queryParamMap, String region, Object data, int length)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException {
    HttpResponse response = execute(Method.PUT, region, bucketName, objectName,
                                    headerMap, queryParamMap,
                                    data, length);
    return response;
  }


  /**
   * Executes PUT method for given request parameters.
   *
   * @param bucketName     Bucket name.
   * @param objectName     Object name in the bucket.
   * @param headerMap      Map of HTTP headers for the request.
   * @param queryParamMap  Map of HTTP query parameters of the request.
   * @param data           HTTP request body data.
   * @param length         Length of HTTP request body data.
   */
  private HttpResponse executePut(String bucketName, String objectName, Map<String,String> headerMap,
                                  Map<String,String> queryParamMap, Object data, int length)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException {
    return executePut(bucketName, objectName, headerMap, queryParamMap, getRegion(bucketName), data, length);
  }


  /**
   * Sets application's name/version to user agent. For more information about user agent
   * refer <a href="http://www.w3.org/Protocols/rfc2616/rfc2616-sec14.html">#rfc2616</a>.
   *
   * @param name     Your application name.
   * @param version  Your application version.
   */
  @SuppressWarnings("unused")
  public void setAppInfo(String name, String version) {
    if (name == null || version == null) {
      // nothing to do
      return;
    }

    this.userAgent = DEFAULT_USER_AGENT + " " + name.trim() + "/" + version.trim();
  }


  /**
   * Returns meta data information of given object in given bucket.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code ObjectStat objectStat = minioClient.statObject("my-bucketname", "my-objectname");
   * System.out.println(objectStat); }</pre>
   *
   * @param bucketName Bucket name.
   * @param objectName Object name in the bucket.
   *
   * @return Populated object metadata.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidResponseException    upon a non-xml response from server
   * @see ObjectStat
   */
  public ObjectStat statObject(String bucketName, String objectName)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException, InvalidArgumentException {
    return statObject(bucketName, objectName, null);
  }

  /**
   * Returns meta data information of given object in given bucket.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code ObjectStat objectStat = minioClient.statObject("my-bucketname", "my-objectname", sse);
   * System.out.println(objectStat); }</pre>
   *
   * @param bucketName Bucket name.
   * @param objectName Object name in the bucket.
   * @param sse        Encryption metadata only required for SSE-C.
   *
   * @return Populated object metadata.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InvalidResponseException    upon a non-xml response from server
   * @see ObjectStat
   */
  public ObjectStat statObject(String bucketName, String objectName, ServerSideEncryption sse)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidArgumentException, InvalidResponseException {
    checkReadRequestSse(sse);

    Map<String, String> headers = null;
    if (sse != null) {
      headers = sse.headers();
    }

    HttpResponse response = executeHead(bucketName, objectName, headers);
    ResponseHeader header = response.header();
    Map<String,List<String>> httpHeaders = response.httpHeaders();
    ObjectStat objectStat = new ObjectStat(bucketName, objectName, header, httpHeaders);
    return objectStat;
  }

  /**
   * Gets object's URL in given bucket.  The URL is ONLY useful to retrieve the object's data if the object has
   * public read permissions.
   *
   * <p><b>Example:</b>
   * <pre>{@code String url = minioClient.getObjectUrl("my-bucketname", "my-objectname");
   * System.out.println(url); }</pre>
   *
   * @param bucketName Bucket name.
   * @param objectName Object name in the bucket.
   *
   * @return string contains URL to download the object.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public String getObjectUrl(String bucketName, String objectName)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException {
    Request request = createRequest(Method.GET, bucketName, objectName, getRegion(bucketName),
        null, null, null, null, 0);
    HttpUrl url = request.url();
    return url.toString();
  }

  /**
   * Gets entire object's data as {@link InputStream} in given bucket. The InputStream must be closed
   * after use else the connection will remain open.
   *
   * <p><b>Example:</b>
   * <pre>{@code InputStream stream = minioClient.getObject("my-bucketname", "my-objectname");
   * byte[] buf = new byte[16384];
   * int bytesRead;
   * while ((bytesRead = stream.read(buf, 0, buf.length)) >= 0) {
   *   System.out.println(new String(buf, 0, bytesRead));
   * }
   * stream.close(); }</pre>
   *
   * @param bucketName Bucket name.
   * @param objectName Object name in the bucket.
   *
   * @return {@link InputStream} containing the object data.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public InputStream getObject(String bucketName, String objectName)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidArgumentException, InvalidResponseException {
    return getObject(bucketName, objectName, null, null, null);
  }


  /**
   * Gets entire object's data as {@link InputStream} in given bucket. The InputStream must be closed
   * after use else the connection will remain open.
   *
   * <p><b>Example:</b>
   * <pre>{@code InputStream stream = minioClient.getObject("my-bucketname", "my-objectname", sse);
   * byte[] buf = new byte[16384];
   * int bytesRead;
   * while ((bytesRead = stream.read(buf, 0, buf.length)) >= 0) {
   *   System.out.println(new String(buf, 0, bytesRead));
   * }
   * stream.close(); }</pre>
   *
   * @param bucketName Bucket name.
   * @param objectName Object name in the bucket.
   * @param sse        Encryption metadata only required for SSE-C.
   *
   * @return {@link InputStream} containing the object data.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public InputStream getObject(String bucketName, String objectName, ServerSideEncryption sse)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidArgumentException, InvalidResponseException {
    return getObject(bucketName, objectName, null, null, sse);
  }


  /**
   * Gets object's data starting from given offset as {@link InputStream} in the given bucket. The InputStream must be
   * closed after use else the connection will remain open.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code InputStream stream = minioClient.getObject("my-bucketname", "my-objectname", 1024L);
   * byte[] buf = new byte[16384];
   * int bytesRead;
   * while ((bytesRead = stream.read(buf, 0, buf.length)) >= 0) {
   *   System.out.println(new String(buf, 0, bytesRead));
   * }
   * stream.close(); }</pre>
   *
   * @param bucketName  Bucket name.
   * @param objectName  Object name in the bucket.
   * @param offset      Offset to read at.
   *
   * @return {@link InputStream} containing the object's data.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public InputStream getObject(String bucketName, String objectName, long offset)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidArgumentException, InvalidResponseException {
    return getObject(bucketName, objectName, offset, null, null);
  }


  /**
   * Gets object's data of given offset and length as {@link InputStream} in the given bucket. The InputStream must be
   * closed after use else the connection will remain open.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code InputStream stream = minioClient.getObject("my-bucketname", "my-objectname", 1024L, 4096L);
   * byte[] buf = new byte[16384];
   * int bytesRead;
   * while ((bytesRead = stream.read(buf, 0, buf.length)) >= 0) {
   *   System.out.println(new String(buf, 0, bytesRead));
   * }
   * stream.close(); }</pre>
   *
   * @param bucketName  Bucket name.
   * @param objectName  Object name in the bucket.
   * @param offset      Offset to read at.
   * @param length      Length to read.
   *
   * @return {@link InputStream} containing the object's data.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public InputStream getObject(String bucketName, String objectName, long offset, Long length)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidArgumentException, InvalidResponseException {
    return getObject(bucketName, objectName, offset, length, null);
  }


  /**
   * Gets object's data of given offset and length as {@link InputStream} in the given bucket. The InputStream must be
   * closed after use else the connection will remain open.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code InputStream stream = minioClient.getObject("my-bucketname", "my-objectname", 1024L, 4096L, sse);
   * byte[] buf = new byte[16384];
   * int bytesRead;
   * while ((bytesRead = stream.read(buf, 0, buf.length)) >= 0) {
   *   System.out.println(new String(buf, 0, bytesRead));
   * }
   * stream.close(); }</pre>
   *
   * @param bucketName  Bucket name.
   * @param objectName  Object name in the bucket.
   * @param offset      Offset to read at.
   * @param length      Length to read.
   * @param sse         Server side encryption.
   *
   * @return {@link InputStream} containing the object's data.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public InputStream getObject(String bucketName, String objectName, Long offset, Long length,
                               ServerSideEncryption sse)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidArgumentException, InvalidResponseException {
    if ((bucketName == null) || (bucketName.isEmpty())) {
      throw new InvalidArgumentException("bucket name cannot be empty");
    }

    if ((objectName == null) || (objectName.isEmpty())) {
      throw new InvalidArgumentException("object name cannot be empty");
    }

    if (offset != null && offset < 0) {
      throw new InvalidArgumentException("offset should be zero or greater");
    }

    if (length != null && length <= 0) {
      throw new InvalidArgumentException("length should be greater than zero");
    }

    checkReadRequestSse(sse);

    if (length != null && offset == null) {
      offset = 0L;
    }

    Map<String,String> headerMap = null;
    if (offset != null || length != null || sse != null) {
      headerMap = new HashMap<>();
    }

    if (length != null) {
      headerMap.put("Range", "bytes=" + offset + "-" + (offset + length - 1));
    } else if (offset != null) {
      headerMap.put("Range", "bytes=" + offset + "-");
    }

    if (sse != null) {
      headerMap.putAll(sse.headers());
    }

    HttpResponse response = executeGet(bucketName, objectName, headerMap, null);
    return response.body().byteStream();
  }


  /**
   * Gets object's data in the given bucket and stores it to given file name.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code minioClient.getObject("my-bucketname", "my-objectname", "photo.jpg"); }</pre>
   *
   * @param bucketName  Bucket name.
   * @param objectName  Object name in the bucket.
   * @param fileName    file name.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public void getObject(String bucketName, String objectName, String fileName)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidArgumentException, InvalidResponseException {
    getObject(bucketName, objectName, null, fileName);
  }


  /**
   * Gets encrypted object's data in the given bucket and stores it to given file name.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code minioClient.getObject("my-bucketname", "my-objectname", sse, "photo.jpg"); }</pre>
   *
   * @param bucketName  Bucket name.
   * @param objectName  Object name in the bucket.
   * @param sse         encryption metadata.
   * @param fileName    file name to download into.
   *
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public void getObject(String bucketName, String objectName, ServerSideEncryption sse, String fileName)
          throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
          InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
          InternalException, InvalidArgumentException, InvalidResponseException {
    checkReadRequestSse(sse);

    Path filePath = Paths.get(fileName);
    boolean fileExists = Files.exists(filePath);

    if (fileExists && !Files.isRegularFile(filePath)) {
      throw new InvalidArgumentException(fileName + ": not a regular file");
    }

    ObjectStat objectStat = statObject(bucketName, objectName, sse);
    long length = objectStat.length();
    String etag = objectStat.etag();

    String tempFileName = fileName + "." + etag + ".part.minio";
    Path tempFilePath = Paths.get(tempFileName);
    boolean tempFileExists = Files.exists(tempFilePath);

    if (tempFileExists && !Files.isRegularFile(tempFilePath)) {
      throw new IOException(tempFileName + ": not a regular file");
    }

    long tempFileSize = 0;
    if (tempFileExists) {
      tempFileSize = Files.size(tempFilePath);
      if (tempFileSize > length) {
        Files.delete(tempFilePath);
        tempFileExists = false;
        tempFileSize = 0;
      }
    }

    if (fileExists) {
      long fileSize = Files.size(filePath);
      if (fileSize == length) {
        // already downloaded. nothing to do
        return;
      } else if (fileSize > length) {
        throw new InvalidArgumentException("Source object, '" + objectName + "', size:" + length
                + " is smaller than the destination file, '" + fileName + "', size:" + fileSize);
      } else if (!tempFileExists) {
        // before resuming the download, copy filename to tempfilename
        Files.copy(filePath, tempFilePath);
        tempFileSize = fileSize;
        tempFileExists = true;
      }
    }

    InputStream is = null;
    OutputStream os = null;
    try {
      is = getObject(bucketName, objectName, tempFileSize, null, sse);
      os = Files.newOutputStream(tempFilePath, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
      long bytesWritten = ByteStreams.copy(is, os);
      is.close();
      os.close();

      if (bytesWritten != length - tempFileSize) {
        throw new IOException(tempFileName + ": unexpected data written.  expected = " + (length - tempFileSize)
                + ", written = " + bytesWritten);
      }
      Files.move(tempFilePath, filePath, StandardCopyOption.REPLACE_EXISTING);
    } finally {
      if (is != null) {
        is.close();
      }
      if (os != null) {
        os.close();
      }
    }
  }


  /**
   * Copy a source object into a new destination object with same object name.
   *
   * </p>
   * <b>Example:</b><br>
   *
   * <pre>
   * {@code minioClient.copyObject("my-bucketname", "my-objectname", "my-destbucketname");}
   * </pre>
   *
   * @param bucketName
   *          Bucket name where the object to be copied exists.
   * @param objectName
   *          Object name source to be copied.
   * @param destBucketName
   *          Bucket name where the object will be copied to.
   *
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws NoResponseException         upon no response from server
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws IOException                 upon connection error
   * @throws XmlPullParserException      upon parsing response xml
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InvalidResponseException    upon a non-xml response from server
   *
   * @deprecated As of release 7.0
   */
  @Deprecated
  public void copyObject(String bucketName, String objectName, String destBucketName)
      throws InvalidKeyException, InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException,
      NoResponseException, ErrorResponseException, InternalException, IOException, XmlPullParserException,
      InvalidArgumentException, InvalidResponseException {
    copyObject(destBucketName, objectName, null, null, bucketName, objectName, null, null);
  }

  /**
   * Copy a source object into a new destination object.
   *
   * </p>
   * <b>Example:</b><br>
   *
   * <pre>
   * {@code minioClient.copyObject("my-bucketname", "my-objectname", "my-destbucketname", "my-destobjname");}
   * </pre>
   *
   * @param bucketName
   *          Bucket name where the object to be copied exists.
   * @param objectName
   *          Object name source to be copied.
   * @param destBucketName
   *          Bucket name where the object will be copied to.
   * @param destObjectName
   *          Object name to be created, if not provided uses source object name
   *          as destination object name.
   *
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws NoResponseException         upon no response from server
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws IOException                 upon connection error
   * @throws XmlPullParserException      upon parsing response xml
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InvalidResponseException    upon a non-xml response from server
   *
   * @deprecated As of release 7.0
   */
  @Deprecated
  public void copyObject(String bucketName, String objectName, String destBucketName, String destObjectName)
      throws InvalidKeyException, InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException,
      NoResponseException, ErrorResponseException, InternalException, IOException, XmlPullParserException,
      InvalidArgumentException, InvalidResponseException {
    if (destObjectName == null) {
      destObjectName = objectName;
    }
    copyObject(destBucketName, destObjectName, null, null, bucketName, objectName, null, null);
  }

  /**
   * Copy a source object into a new object with the provided name in the provided bucket.
   * optionally can take a key value CopyConditions as well for conditionally attempting
   * copyObject.
   *
   * </p>
   * <b>Example:</b><br>
   *
   * <pre>
   * {@code minioClient.copyObject("my-bucketname", "my-objectname", "my-destbucketname",
   * copyConditions);}
   * </pre>
   *
   * @param bucketName
   *          Bucket name where the object to be copied exists.
   * @param objectName
   *          Object name source to be copied.
   * @param destBucketName
   *          Bucket name where the object will be copied to.
   * @param copyConditions
   *          CopyConditions object with collection of supported CopyObject conditions.
   *
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws NoResponseException         upon no response from server
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws IOException                 upon connection error
   * @throws XmlPullParserException      upon parsing response xml
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InvalidResponseException    upon a non-xml response from server
   *
   * @deprecated As of release 7.0
   */
  @Deprecated
  public void copyObject(String bucketName, String objectName, String destBucketName, CopyConditions copyConditions)
        throws InvalidKeyException, InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException,
        NoResponseException, ErrorResponseException, InternalException, IOException, XmlPullParserException,
        InvalidArgumentException, InvalidResponseException {
    copyObject(destBucketName, objectName, null, null, bucketName, objectName, null, copyConditions);
  }

  /**
   * Copy a source object into a new object with the provided name in the provided bucket.
   * optionally can take a key value CopyConditions as well for conditionally attempting
   * copyObject.
   *
   * </p>
   * <b>Example:</b><br>
   *
   * <pre>
   * {@code minioClient.copyObject("my-bucketname", "my-objectname", "my-destbucketname",
   * "my-destobjname", copyConditions);}
   * </pre>
   *
   * @param bucketName
   *          Bucket name where the object to be copied exists.
   * @param objectName
   *          Object name source to be copied.
   * @param destBucketName
   *          Bucket name where the object will be copied to.
   * @param destObjectName
   *          Object name to be created, if not provided uses source object name
   *          as destination object name.
   * @param copyConditions
   *          CopyConditions object with collection of supported CopyObject conditions.
   *
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws NoResponseException         upon no response from server
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws IOException                 upon connection error
   * @throws XmlPullParserException      upon parsing response xml
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InvalidResponseException    upon a non-xml response from server
   *
   * @deprecated As of release 7.0
   */
  @Deprecated
  public void copyObject(String bucketName, String objectName, String destBucketName, String destObjectName,
                         CopyConditions copyConditions)
      throws InvalidKeyException, InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException,
      NoResponseException, ErrorResponseException, InternalException, IOException, XmlPullParserException,
      InvalidArgumentException, InvalidResponseException {
    if (destObjectName == null) {
      destObjectName = objectName;
    }
    copyObject(destBucketName, destObjectName, null, null, bucketName, objectName, null, copyConditions);
  }

  /**
   * Copy a source object into a new object with the provided name in the provided bucket.
   * optionally can take a key value CopyConditions as well for conditionally attempting
   * copyObject.
   *
   * </p>
   * <b>Example:</b><br>
   *
   * <pre>
   * {@code minioClient.copyObject("my-bucketname", "my-objectname", sseSource, "my-destbucketname",
   * "my-destobjname", copyConditions, sseTarget);}
   * </pre>
   *
   * @param bucketName
   *          Bucket name where the object to be copied exists.
   * @param objectName
   *          Object name source to be copied.
   * @param destBucketName
   *          Bucket name where the object will be copied to.
   * @param destObjectName
   *          Object name to be created, if not provided uses source object name
   *          as destination object name.
   * @param copyConditions
   *          CopyConditions object with collection of supported CopyObject conditions.
   * @param sseSource
   *          Source Encryption metadata.
   * @param sseTarget
   *          Target Encryption metadata.
   *
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws NoResponseException         upon no response from server
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws IOException                 upon connection error
   * @throws XmlPullParserException      upon parsing response xml
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InvalidResponseException    upon a non-xml response from server
   *
   * @deprecated As of release 7.0
   */
  @Deprecated
  public void copyObject(String bucketName, String objectName, ServerSideEncryption sseSource, String destBucketName,
                         String destObjectName, CopyConditions copyConditions, ServerSideEncryption sseTarget)
      throws InvalidKeyException, InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException,
      NoResponseException, ErrorResponseException, InternalException, IOException, XmlPullParserException,
      InvalidArgumentException, InvalidResponseException {
    if (destObjectName == null) {
      destObjectName = objectName;
    }
    copyObject(destBucketName, destObjectName, null, sseTarget, bucketName, objectName, sseSource, copyConditions);
  }

  /**
   * Copy a source object into a new object with the provided name in the provided bucket.
   * optionally can take a key value CopyConditions as well for conditionally attempting
   * copyObject.
   *
   * </p>
   * <b>Example:</b><br>
   *
   * <pre>
   * {@code minioClient.copyObject("my-bucketname", "my-objectname", "my-destbucketname",
   * "my-destobjname", copyConditions, metadata);}
   * </pre>
   *
   * @param bucketName
   *          Bucket name where the object to be copied exists.
   * @param objectName
   *          Object name source to be copied.
   * @param destBucketName
   *          Bucket name where the object will be copied to.
   * @param destObjectName
   *          Object name to be created, if not provided uses source object name
   *          as destination object name.
   * @param copyConditions
   *          CopyConditions object with collection of supported CopyObject conditions.
   * @param metadata
   *          Additional metadata to set on the destination object when
   *          setMetadataDirective is set to 'REPLACE'.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InvalidResponseException    upon a non-xml response from server
   *
   * @deprecated As of release 7.0
   */
  @Deprecated
  public void copyObject(String bucketName, String objectName, String destBucketName,
                         String destObjectName, CopyConditions copyConditions,
                         Map<String,String> metadata)
      throws InvalidKeyException, InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException,
      NoResponseException, ErrorResponseException, InternalException, IOException, XmlPullParserException,
      InvalidArgumentException, InvalidResponseException {
    if (destObjectName == null) {
      destObjectName = objectName;
    }
    copyObject(destBucketName, destObjectName, metadata, null, bucketName, objectName, null, copyConditions);
  }


  /**
   * Copy a source object into a new object with the provided name in the provided bucket.
   * optionally can take a key value CopyConditions and server side encryption as well for
   * conditionally attempting copyObject.
   *
   * </p>
   * <b>Example:</b><br>
   *
   * <pre>
   * {@code minioClient.copyObject("my-bucketname", "my-objectname", headers, sse, "my-srcbucketname",
   * "my-srcobjname", srcSse, copyConditions);}
   * </pre>
   *
   * @param bucketName
   *          Destination bucket name.
   * @param objectName
   *          Destination object name.
   * @param headerMap
   *          Destination object custom metadata.
   * @param sse
   *          Server side encryption of destination object.
   * @param srcBucketName
   *          Source bucket name.
   * @param srcObjectName
   *          Source object name.
   * @param srcSse
   *          Server side encryption of source object.
   * @param copyConditions
   *          CopyConditions object with collection of supported CopyObject conditions.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public void copyObject(String bucketName, String objectName, Map<String,String> headerMap, ServerSideEncryption sse,
                         String srcBucketName, String srcObjectName, ServerSideEncryption srcSse,
                         CopyConditions copyConditions)
    throws InvalidKeyException, InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException,
           NoResponseException, ErrorResponseException, InternalException, IOException, XmlPullParserException,
           InvalidArgumentException, InvalidResponseException {
    if (bucketName == null) {
      throw new InvalidArgumentException("bucket name cannot be empty");
    }

    if (objectName == null) {
      throw new InvalidArgumentException("object name cannot be empty");
    }

    checkWriteRequestSse(sse);

    if (srcBucketName == null) {
      throw new InvalidArgumentException("Source bucket name cannot be empty");
    }

    // Source object name is optional, if empty default to object name.
    if (srcObjectName == null) {
      srcObjectName = objectName;
    }

    checkReadRequestSse(srcSse);

    if (headerMap == null) {
      headerMap = new HashMap<>();
    }

    headerMap.put("x-amz-copy-source", S3Escaper.encodePath(srcBucketName + "/" + srcObjectName));

    if (sse != null) {
      headerMap.putAll(sse.headers());
    }

    if (srcSse != null) {
      headerMap.putAll(srcSse.copySourceHeaders());
    }

    if (copyConditions != null) {
      headerMap.putAll(copyConditions.getConditions());
    }

    HttpResponse response = executePut(bucketName, objectName, headerMap, null, "", 0);

    // For now ignore the copyObjectResult, just read and parse it.
    CopyObjectResult result = new CopyObjectResult();
    try (ResponseBody body = response.body()) {
      result.parseXml(body.charStream());
    }
  }

  /**
   * Create an object by concatenating a list of source objects using server-side copying.
   * </p>
   * <b>Example:</b><br>   *
   * <pre>
   * {@code minioClient.composeObject(String bucketName, String objectName,
   * List<ComposeSource> composeSource, Map<String,String> userMetaData,
   * ServiceConfigurationError sse );}
   * </pre>
   *
   * @param bucketName
   *          Destination Bucket to be created upon compose.
   * @param objectName
   *          Destination Object to be created upon compose.
   * @param sources
   *          List of Source Objects used to compose Object.
   * @param headerMap
   *          User Meta data.
   * @param sse
   *          Server Side Encryption.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InvalidResponseException    upon a non-xml response from server
   */

  public void composeObject(String bucketName, String objectName, List<ComposeSource> sources,
      Map<String,String> headerMap, ServerSideEncryption sse)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
    InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
    InternalException, InvalidArgumentException, InvalidResponseException {
    if (bucketName == null) {
      throw new InvalidArgumentException("Destination bucket name cannot be empty");
    }

    if (objectName == null) {
      throw new InvalidArgumentException("Destination object name cannot be empty");
    }

    if (sources.isEmpty()) {
      throw new InvalidArgumentException("compose sources cannot be empty");
    }

    checkWriteRequestSse(sse);

    long objectSize = 0;
    int partsCount = 0;
    for (int i = 0; i < sources.size(); i++) {
      ComposeSource src = sources.get(i);

      checkReadRequestSse(src.sse());

      ObjectStat stat = statObject(src.bucketName(), src.objectName(), src.sse());
      src.buildHeaders(stat.length(), stat.etag());

      if (i != 0 && src.headers().containsKey("x-amz-meta-x-amz-key")) {
        throw new InvalidArgumentException("Client side encryption is not supported for more than one source");
      }

      long size = stat.length();
      if (src.length() != null) {
        size = src.length();
      } else if (src.offset() != null) {
        size -= src.offset();
      }

      if (size < MIN_MULTIPART_SIZE && sources.size() != 1 && i != (sources.size() - 1)) {
        throw new InvalidArgumentException("source " + src.bucketName() + "/" + src.objectName() + ": size "
          + size + " must be greater than " + MIN_MULTIPART_SIZE);
      }

      objectSize += size;
      if (objectSize > MAX_OBJECT_SIZE) {
        throw new InvalidArgumentException("Destination object size must be less than " + MAX_OBJECT_SIZE);
      }

      if (size > MAX_PART_SIZE) {
        long count = size / MAX_PART_SIZE;
        long lastPartSize = size - (count * MAX_PART_SIZE);
        if (lastPartSize > 0) {
          count++;
        } else {
          lastPartSize = MAX_PART_SIZE;
        }

        if (lastPartSize < MIN_MULTIPART_SIZE && sources.size() != 1 && i != (sources.size() - 1)) {
          throw new InvalidArgumentException("source " + src.bucketName() + "/" + src.objectName() + ": "
            + "for multipart split upload of " + size
            + ", last part size is less than " + MIN_MULTIPART_SIZE);
        }

        partsCount += (int)count;
      } else {
        partsCount++;
      }

      if (partsCount > MAX_MULTIPART_COUNT) {
        throw new InvalidArgumentException("Compose sources create more than allowed multipart count "
          + MAX_MULTIPART_COUNT);
      }
    }

    if (partsCount == 1) {
      ComposeSource src = sources.get(0);
      if (headerMap == null) {
        headerMap = new HashMap<>();
      }
      if ((src.offset() != null) && ( src.length() == null)) {
        headerMap.put("x-amz-copy-source-range", "bytes=" + src.offset() + "-" );
      }

      if ((src.offset() != null) && ( src.length() != null)) {
        headerMap.put("x-amz-copy-source-range", "bytes=" + src.offset() + "-" + (src.offset() + src.length() - 1));
      }
      copyObject(bucketName, objectName, headerMap, sse, src.bucketName(), src.objectName(), src.sse(),
          src.copyConditions());
      return;
    }

    Map<String, String> sseHeaders = null;
    if (sse != null) {
      sseHeaders = sse.headers();
      if (headerMap == null) {
        headerMap = new HashMap<>();
      }
      headerMap.putAll(sseHeaders);
    }

    String uploadId = initMultipartUpload(bucketName, objectName, headerMap);

    int partNumber = 0;
    Part[] totalParts = new Part[partsCount];
    try {
      for (int i = 0; i < sources.size(); i++) {
        ComposeSource src = sources.get(i);

        long size = src.objectSize();
        if (src.length() != null) {
          size = src.length();
        } else if (src.offset() != null) {
          size -= src.offset();
        }
        long offset = 0;
        if (src.offset() != null) {
          offset = src.offset();
        }

        if (size <= MAX_PART_SIZE) {
          partNumber++;
          Map<String, String> headers = null;
          if (src.headers() == null) {
            headers = new HashMap<>();
          } else {
            headers = src.headers();
          }
          if (src.length() != null) {
            headers.put("x-amz-copy-source-range", "bytes=" + offset + "-" + (offset + src.length() - 1));
          } else if (src.offset() != null) {
            headers.put("x-amz-copy-source-range", "bytes=" + offset + "-" + (offset + size - 1));
          }
          if (sseHeaders != null) {
            headers.putAll(sseHeaders);
          }
          String eTag = uploadPartCopy(bucketName, objectName, uploadId, partNumber, headers);

          totalParts[partNumber - 1] = new Part(partNumber, eTag);
          continue;
        }

        while (size > 0) {
          partNumber++;

          long startBytes = offset;
          long endBytes = startBytes + MAX_PART_SIZE;
          if (size < MAX_PART_SIZE) {
            endBytes = startBytes + size;
          }

          Map<String, String> headers = src.headers();
          headers.put("x-amz-copy-source-range", "bytes=" + startBytes + "-" + endBytes);
          if (sseHeaders != null) {
            headers.putAll(sseHeaders);
          }
          String eTag = uploadPartCopy(bucketName, objectName, uploadId, partNumber, headers);

          totalParts[partNumber - 1] = new Part(partNumber, eTag);

          offset = startBytes;
          size -= (endBytes - startBytes);
        }
      }

      completeMultipart(bucketName, objectName, uploadId, totalParts);
    } catch (RuntimeException e) {
      abortMultipartUpload(bucketName, objectName, uploadId);
      throw e;
    } catch (Exception e) {
      abortMultipartUpload(bucketName, objectName, uploadId);
      throw e;
    }
  }

  private String uploadPartCopy(String bucketName, String objectName, String uploadId, int partNumber,
      Map<String, String> headerMap)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
    InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
    InternalException, InvalidResponseException {
    Map<String,String> queryParamMap = new HashMap<>();
    queryParamMap.put("partNumber", Integer.toString(partNumber));
    queryParamMap.put("uploadId", uploadId);
    HttpResponse response = executePut(bucketName, objectName, headerMap, queryParamMap, "", 0);
    CopyPartResult result = new CopyPartResult();
    try (ResponseBody body = response.body()) {
      result.parseXml(body.charStream());
    }
    return result.etag();
  }


  /**
   * Returns a presigned URL string with given HTTP method, expiry time and custom request params for a 
   * specific object in the bucket.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code String url = minioClient.getPresignedObjectUrl(Method.DELETE, "my-bucketname", "my-objectname",
   * 60 * 60 * 24, reqParams);
   * System.out.println(url); }</pre>
   *
   * @param method      HTTP {@link Method}.
   * @param bucketName  Bucket name.
   * @param objectName  Object name in the bucket.
   * @param expires     Expiration time in seconds of presigned URL.
   * @param reqParams   Override values for set of response headers. Currently supported request parameters are
   *                    [response-expires, response-content-type, response-cache-control, response-content-disposition]
   *
   * @return string contains URL to download the object.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidExpiresRangeException upon input expires is out of range
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public String getPresignedObjectUrl(Method method, String bucketName, String objectName, Integer expires,
                                      Map<String, String> reqParams)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidExpiresRangeException, InvalidResponseException {
    // Validate input.
    if (expires < 1 || expires > DEFAULT_EXPIRY_TIME) {
      throw new InvalidExpiresRangeException(expires, "expires must be in range of 1 to " + DEFAULT_EXPIRY_TIME);
    }

    byte[] body = null;
    if (method == Method.PUT || method == Method.POST) {
      body = new byte[0];
    }

    Multimap<String, String> queryParamMap = null;
    if (reqParams != null) {
      queryParamMap = HashMultimap.create();
      for (Map.Entry<String, String> m: reqParams.entrySet()) {
        queryParamMap.put(m.getKey(), m.getValue());
      }
    }

    String region = getRegion(bucketName);
    Request request = createRequest(method, bucketName, objectName, region, null, queryParamMap, null, body, 0);
    HttpUrl url = Signer.presignV4(request, region, accessKey, secretKey, expires);
    return url.toString();
  }

  /**
   * Returns an presigned URL to download the object in the bucket with given expiry time with custom request params.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code String url = minioClient.presignedGetObject("my-bucketname", "my-objectname", 60 * 60 * 24, reqParams);
   * System.out.println(url); }</pre>
   *
   * @param bucketName  Bucket name.
   * @param objectName  Object name in the bucket.
   * @param expires     Expiration time in seconds of presigned URL.
   * @param reqParams   Override values for set of response headers. Currently supported request parameters are
   *                    [response-expires, response-content-type, response-cache-control, response-content-disposition]
   *
   * @return string contains URL to download the object.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidExpiresRangeException upon input expires is out of range
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public String presignedGetObject(String bucketName, String objectName, Integer expires,
                                   Map<String, String> reqParams)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidExpiresRangeException, InvalidResponseException {
    return getPresignedObjectUrl(Method.GET, bucketName, objectName, expires, reqParams);
  }

  /**
   * Returns an presigned URL to download the object in the bucket with given expiry time.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code String url = minioClient.presignedGetObject("my-bucketname", "my-objectname", 60 * 60 * 24);
   * System.out.println(url); }</pre>
   *
   * @param bucketName  Bucket name.
   * @param objectName  Object name in the bucket.
   * @param expires     Expiration time in seconds of presigned URL.
   *
   * @return string contains URL to download the object.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidExpiresRangeException upon input expires is out of range
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public String presignedGetObject(String bucketName, String objectName, Integer expires)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidExpiresRangeException, InvalidResponseException {
    return presignedGetObject(bucketName, objectName, expires, null);
  }


  /**
   * Returns an presigned URL to download the object in the bucket with default expiry time.
   * Default expiry time is 7 days in seconds.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code String url = minioClient.presignedGetObject("my-bucketname", "my-objectname");
   * System.out.println(url); }</pre>
   *
   * @param bucketName  Bucket name.
   * @param objectName  Object name in the bucket.
   *
   * @return string contains URL to download the object
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidExpiresRangeException upon input expires is out of range
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public String presignedGetObject(String bucketName, String objectName)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidExpiresRangeException, InvalidResponseException {
    return presignedGetObject(bucketName, objectName, DEFAULT_EXPIRY_TIME, null);
  }


  /**
   * Returns a presigned URL to upload an object in the bucket with given expiry time.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code String url = minioClient.presignedPutObject("my-bucketname", "my-objectname", 60 * 60 * 24);
   * System.out.println(url); }</pre>
   *
   * @param bucketName  Bucket name
   * @param objectName  Object name in the bucket
   * @param expires     Expiration time in seconds to presigned URL.
   *
   * @return string contains URL to upload the object.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidExpiresRangeException upon input expires is out of range
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public String presignedPutObject(String bucketName, String objectName, Integer expires)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidExpiresRangeException, InvalidResponseException {
    return getPresignedObjectUrl(Method.PUT, bucketName, objectName, expires, null);
  }


  /**
   * Returns a presigned URL to upload an object in the bucket with default expiry time.
   * Default expiry time is 7 days in seconds.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code String url = minioClient.presignedPutObject("my-bucketname", "my-objectname");
   * System.out.println(url); }</pre>
   *
   * @param bucketName  Bucket name.
   * @param objectName  Object name in the bucket.
   *
   * @return string contains URL to upload the object.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidExpiresRangeException upon input expires is out of range
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public String presignedPutObject(String bucketName, String objectName)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidExpiresRangeException, InvalidResponseException {
    return presignedPutObject(bucketName, objectName, DEFAULT_EXPIRY_TIME);
  }


  /**
   * Returns string map for given {@link PostPolicy} to upload object with various post policy conditions.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code // Create new PostPolicy object for 'my-bucketname', 'my-objectname' and 7 days expire time from now.
   * PostPolicy policy = new PostPolicy("my-bucketname", "my-objectname", DateTime.now().plusDays(7));
   * // 'my-objectname' should be 'image/png' content type
   * policy.setContentType("image/png");
   * Map<String,String> formData = minioClient.presignedPostPolicy(policy);
   * // Print a curl command that can be executable with the file /tmp/userpic.png and the file will be uploaded.
   * System.out.print("curl -X POST ");
   * for (Map.Entry<String,String> entry : formData.entrySet()) {
   *   System.out.print(" -F " + entry.getKey() + "=" + entry.getValue());
   * }
   * System.out.println(" -F file=@/tmp/userpic.png https://play.min.io/my-bucketname"); }</pre>
   *
   * @param policy Post policy of an object.
   * @return Map of strings to construct form-data.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InvalidResponseException    upon a non-xml response from server
   * @see PostPolicy
   */
  public Map<String, String> presignedPostPolicy(PostPolicy policy)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidArgumentException, InvalidResponseException {
    return policy.formData(this.accessKey, this.secretKey, getRegion(policy.bucketName()));
  }


  /**
   * Removes an object from a bucket.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code minioClient.removeObject("my-bucketname", "my-objectname"); }</pre>
   *
   * @param bucketName Bucket name.
   * @param objectName Object name in the bucket.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public void removeObject(String bucketName, String objectName)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidArgumentException, InvalidResponseException {
    if ((bucketName == null) || (bucketName.isEmpty())) {
      throw new InvalidArgumentException("bucket name cannot be empty");
    }

    if ((objectName == null) || (objectName.isEmpty())) {
      throw new InvalidArgumentException("object name cannot be empty");
    }

    executeDelete(bucketName, objectName, null);
  }


  private List<DeleteError> removeObject(String bucketName, List<DeleteObject> objectList)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException {
    Map<String,String> queryParamMap = new HashMap<>();
    queryParamMap.put("delete", "");

    DeleteRequest request = new DeleteRequest(objectList);
    HttpResponse response = executePost(bucketName, null, null, queryParamMap, request);

    String bodyContent = "";
    // Use scanner to read entire body stream to string.
    Scanner scanner = new Scanner(response.body().charStream());
    try {
      scanner.useDelimiter("\\A");
      if (scanner.hasNext()) {
        bodyContent = scanner.next();
      }
    } finally {
      response.body().close();
      scanner.close();
    }

    List<DeleteError> errorList = null;

    bodyContent = bodyContent.trim();
    // Check if body content is <Error> message.
    DeleteError error = new DeleteError(new StringReader(bodyContent));
    if (error.code() != null) {
      // As it is <Error> message, add to error list.
      errorList = new LinkedList<DeleteError>();
      errorList.add(error);
    } else {
      // As it is not <Error> message, parse it as <DeleteResult> message.
      DeleteResult result = new DeleteResult(new StringReader(bodyContent));
      errorList = result.errorList();
    }

    return errorList;
  }


  /**
   * Removes multiple objects from a bucket.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code // Create object list for removal.
   * List<String> objectNames = new LinkedList<String>();
   * objectNames.add("my-objectname1");
   * objectNames.add("my-objectname2");
   * objectNames.add("my-objectname3");
   * for (Result<DeleteError> errorResult: minioClient.removeObjects("my-bucketname", objectNames)) {
   *     DeleteError error = errorResult.get();
   *     System.out.println("Failed to remove '" + error.objectName() + "'. Error:" + error.message());
   * } }</pre>
   *
   * @param bucketName Bucket name.
   * @param objectNames List of Object names in the bucket.
   */
  public Iterable<Result<DeleteError>> removeObjects(final String bucketName, final Iterable<String> objectNames) {
    return new Iterable<Result<DeleteError>>() {
      @Override
      public Iterator<Result<DeleteError>> iterator() {
        return new Iterator<Result<DeleteError>>() {
          private Result<DeleteError> error;
          private Iterator<DeleteError> errorIterator;
          private boolean completed = false;
          private Iterator<String> objectNameIter = objectNames.iterator();

          private synchronized void populate() {
            List<DeleteError> errorList = null;
            try {
              List<DeleteObject> objectList = new LinkedList<DeleteObject>();
              int i = 0;
              while (objectNameIter.hasNext() && i < 1000) {
                objectList.add(new DeleteObject(objectNameIter.next()));
                i++;
              }

              if (i > 0) {
                errorList = removeObject(bucketName, objectList);
              }
            } catch (InvalidBucketNameException | NoSuchAlgorithmException | InsufficientDataException | IOException
                     | InvalidKeyException | NoResponseException | XmlPullParserException | ErrorResponseException
                     | InternalException | InvalidResponseException e) {
              this.error = new Result<>(null, e);
            } finally {
              if (errorList != null) {
                this.errorIterator = errorList.iterator();
              } else {
                this.errorIterator = new LinkedList<DeleteError>().iterator();
              }
            }
          }

          @Override
          public boolean hasNext() {
            if (this.completed) {
              return false;
            }

            if (this.error == null && this.errorIterator == null) {
              populate();
            }

            if (this.error == null && this.errorIterator != null && !this.errorIterator.hasNext()) {
              populate();
            }

            if (this.error != null) {
              return true;
            }

            if (this.errorIterator.hasNext()) {
              return true;
            }

            this.completed = true;
            return false;
          }

          @Override
          public Result<DeleteError> next() {
            if (this.completed) {
              throw new NoSuchElementException();
            }

            if (this.error == null && this.errorIterator == null) {
              populate();
            }

            if (this.error == null && this.errorIterator != null && !this.errorIterator.hasNext()) {
              populate();
            }

            if (this.error != null) {
              this.completed = true;
              return this.error;
            }

            if (this.errorIterator.hasNext()) {
              return new Result<>(this.errorIterator.next(), null);
            }

            this.completed = true;
            throw new NoSuchElementException();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  /**
   * Lists object information in given bucket.
   *
   * @param bucketName Bucket name.
   *
   * @return an iterator of Result Items.
   *
   ** @throws XmlPullParserException      upon parsing response xml
   */
  public Iterable<Result<Item>> listObjects(final String bucketName) throws XmlPullParserException {
    return listObjects(bucketName, null);
  }


  /**
   * Lists object information in given bucket and prefix.
   *
   * @param bucketName Bucket name.
   * @param prefix     Prefix string.  List objects whose name starts with `prefix`.
   *
   * @return an iterator of Result Items.
   *
   * @throws XmlPullParserException      upon parsing response xml
   */
  public Iterable<Result<Item>> listObjects(final String bucketName, final String prefix)
    throws XmlPullParserException {
    // list all objects recursively
    return listObjects(bucketName, prefix, true);
  }


  /**
   * Lists object information as {@code Iterable<Result><Item>} in given bucket, prefix and recursive flag.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code Iterable<Result<Item>> myObjects = minioClient.listObjects("my-bucketname");
   * for (Result<Item> result : myObjects) {
   *   Item item = result.get();
   *   System.out.println(item.lastModified() + ", " + item.size() + ", " + item.objectName());
   * } }</pre>
   *
   * @param bucketName Bucket name.
   * @param prefix     Prefix string.  List objects whose name starts with `prefix`.
   * @param recursive when false, emulates a directory structure where each listing returned is either a full object
   *                  or part of the object's key up to the first '/'. All objects wit the same prefix up to the first
   *                  '/' will be merged into one entry.
   *
   * @return an iterator of Result Items.
   *
//   * @see #listObjects(String bucketName)
//   * @see #listObjects(String bucketName, String prefix)
//   * @see #listObjects(String bucketName, String prefix, boolean recursive, boolean useVersion1)
   */
  public Iterable<Result<Item>> listObjects(final String bucketName, final String prefix, final boolean recursive) {
    return listObjects(bucketName, prefix, recursive, false);
  }


  /**
   * Lists object information as {@code Iterable<Result><Item>} in given bucket, prefix, recursive flag and S3 API
   * version to use.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code Iterable<Result<Item>> myObjects = minioClient.listObjects("my-bucketname", "my-object-prefix", true,
   *                                    false);
   * for (Result<Item> result : myObjects) {
   *   Item item = result.get();
   *   System.out.println(item.lastModified() + ", " + item.size() + ", " + item.objectName());
   * } }</pre>
   *
   * @param bucketName Bucket name.
   * @param prefix     Prefix string.  List objects whose name starts with `prefix`.
   * @param recursive when false, emulates a directory structure where each listing returned is either a full object
   *                  or part of the object's key up to the first '/'. All objects wit the same prefix up to the first
   *                  '/' will be merged into one entry.
   * @param useVersion1 If set, Amazon AWS S3 List Object V1 is used, else List Object V2 is used as default.
   *
   * @return an iterator of Result Items.
   *
//   * @see #listObjects(String bucketName)
//   * @see #listObjects(String bucketName, String prefix)
//   * @see #listObjects(String bucketName, String prefix, boolean recursive)
   */
  public Iterable<Result<Item>> listObjects(final String bucketName, final String prefix, final boolean recursive,
                                            final boolean useVersion1) {
    if (useVersion1) {
      return listObjectsV1(bucketName, prefix, recursive);
    }

    return listObjectsV2(bucketName, prefix, recursive);
  }


  private Iterable<Result<Item>> listObjectsV2(final String bucketName, final String prefix, final boolean recursive) {
    return new Iterable<Result<Item>>() {
      @Override
      public Iterator<Result<Item>> iterator() {
        return new Iterator<Result<Item>>() {
          private ListBucketResult listBucketResult;
          private Result<Item> error;
          private Iterator<Item> itemIterator;
          private Iterator<Prefix> prefixIterator;
          private boolean completed = false;

          private synchronized void populate() {
            String delimiter = "/";
            if (recursive) {
              delimiter = null;
            }

            String continuationToken = null;
            if (this.listBucketResult != null) {
              continuationToken = listBucketResult.nextContinuationToken();
            }

            this.listBucketResult = null;
            this.itemIterator = null;
            this.prefixIterator = null;

            try {
              this.listBucketResult = listObjectsV2(bucketName, continuationToken, prefix, delimiter);
            } catch (InvalidBucketNameException | NoSuchAlgorithmException | InsufficientDataException | IOException
                     | InvalidKeyException | NoResponseException | XmlPullParserException | ErrorResponseException
                     | InternalException | InvalidResponseException e) {
              this.error = new Result<>(null, e);
            } finally {
              if (this.listBucketResult != null) {
                this.itemIterator = this.listBucketResult.contents().iterator();
                this.prefixIterator = this.listBucketResult.commonPrefixes().iterator();
              } else {
                this.itemIterator = new LinkedList<Item>().iterator();
                this.prefixIterator = new LinkedList<Prefix>().iterator();
              }
            }
          }

          @Override
          public boolean hasNext() {
            if (this.completed) {
              return false;
            }

            if (this.error == null && this.itemIterator == null && this.prefixIterator == null) {
              populate();
            }

            if (this.error == null && !this.itemIterator.hasNext() && !this.prefixIterator.hasNext()
                && this.listBucketResult.isTruncated()) {
              populate();
            }

            if (this.error != null) {
              return true;
            }

            if (this.itemIterator.hasNext()) {
              return true;
            }

            if (this.prefixIterator.hasNext()) {
              return true;
            }

            this.completed = true;
            return false;
          }

          @Override
          public Result<Item> next() {
            if (this.completed) {
              throw new NoSuchElementException();
            }

            if (this.error == null && this.itemIterator == null && this.prefixIterator == null) {
              populate();
            }

            if (this.error == null && !this.itemIterator.hasNext() && !this.prefixIterator.hasNext()
                && this.listBucketResult.isTruncated()) {
              populate();
            }

            if (this.error != null) {
              this.completed = true;
              return this.error;
            }

            if (this.itemIterator.hasNext()) {
              Item item = this.itemIterator.next();
              return new Result<>(item, null);
            }

            if (this.prefixIterator.hasNext()) {
              Prefix prefix = this.prefixIterator.next();
              Item item;
              try {
                item = new Item(prefix.prefix(), true);
              } catch (XmlPullParserException e) {
                // special case: ignore the error as we can't propagate the exception in next()
                item = null;
              }

              return new Result<>(item, null);
            }

            this.completed = true;
            throw new NoSuchElementException();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }


  /**
   * Returns {@link ListBucketResult} of given bucket, marker, prefix and delimiter.
   *
   * @param bucketName        Bucket name.
   * @param continuationToken Marker string.  List objects whose name is greater than `marker`.
   * @param prefix            Prefix string.  List objects whose name starts with `prefix`.
   * @param delimiter         Delimiter string.  Group objects whose name contains `delimiter`.
   */
  private ListBucketResult listObjectsV2(String bucketName, String continuationToken, String prefix, String delimiter)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException {
    Map<String,String> queryParamMap = new HashMap<>();
    queryParamMap.put("list-type", "2");

    if (continuationToken != null) {
      queryParamMap.put("continuation-token", continuationToken);
    }

    if (prefix != null) {
      queryParamMap.put("prefix", prefix);
    } else {
      queryParamMap.put("prefix", "");
    }

    if (delimiter != null) {
      queryParamMap.put("delimiter", delimiter);
    } else {
      queryParamMap.put("delimiter", "");
    }

    HttpResponse response = executeGet(bucketName, null, null, queryParamMap);

    ListBucketResult result = new ListBucketResult();
    result.parseXml(response.body().charStream());
    response.body().close();
    return result;
  }


  private Iterable<Result<Item>> listObjectsV1(final String bucketName, final String prefix, final boolean recursive) {
    return new Iterable<Result<Item>>() {
      @Override
      public Iterator<Result<Item>> iterator() {
        return new Iterator<Result<Item>>() {
          private String lastObjectName;
          private ListBucketResultV1 listBucketResult;
          private Result<Item> error;
          private Iterator<Item> itemIterator;
          private Iterator<Prefix> prefixIterator;
          private boolean completed = false;

          private synchronized void populate() {
            String delimiter = "/";
            if (recursive) {
              delimiter = null;
            }

            String marker = null;
            if (this.listBucketResult != null) {
              if (delimiter != null) {
                marker = listBucketResult.nextMarker();
              } else {
                marker = this.lastObjectName;
              }
            }

            this.listBucketResult = null;
            this.itemIterator = null;
            this.prefixIterator = null;

            try {
              this.listBucketResult = listObjectsV1(bucketName, marker, prefix, delimiter);
            } catch (InvalidBucketNameException | NoSuchAlgorithmException | InsufficientDataException | IOException
                     | InvalidKeyException | NoResponseException | XmlPullParserException | ErrorResponseException
                     | InternalException | InvalidResponseException e) {
              this.error = new Result<>(null, e);
            } finally {
              if (this.listBucketResult != null) {
                this.itemIterator = this.listBucketResult.contents().iterator();
                this.prefixIterator = this.listBucketResult.commonPrefixes().iterator();
              } else {
                this.itemIterator = new LinkedList<Item>().iterator();
                this.prefixIterator = new LinkedList<Prefix>().iterator();
              }
            }
          }

          @Override
          public boolean hasNext() {
            if (this.completed) {
              return false;
            }

            if (this.error == null && this.itemIterator == null && this.prefixIterator == null) {
              populate();
            }

            if (this.error == null && !this.itemIterator.hasNext() && !this.prefixIterator.hasNext()
                && this.listBucketResult.isTruncated()) {
              populate();
            }

            if (this.error != null) {
              return true;
            }

            if (this.itemIterator.hasNext()) {
              return true;
            }

            if (this.prefixIterator.hasNext()) {
              return true;
            }

            this.completed = true;
            return false;
          }

          @Override
          public Result<Item> next() {
            if (this.completed) {
              throw new NoSuchElementException();
            }

            if (this.error == null && this.itemIterator == null && this.prefixIterator == null) {
              populate();
            }

            if (this.error == null && !this.itemIterator.hasNext() && !this.prefixIterator.hasNext()
                && this.listBucketResult.isTruncated()) {
              populate();
            }

            if (this.error != null) {
              this.completed = true;
              return this.error;
            }

            if (this.itemIterator.hasNext()) {
              Item item = this.itemIterator.next();
              this.lastObjectName = item.objectName();
              return new Result<>(item, null);
            }

            if (this.prefixIterator.hasNext()) {
              Prefix prefix = this.prefixIterator.next();
              Item item;
              try {
                item = new Item(prefix.prefix(), true);
              } catch (XmlPullParserException e) {
                // special case: ignore the error as we can't propagate the exception in next()
                item = null;
              }

              return new Result<>(item, null);
            }

            this.completed = true;
            throw new NoSuchElementException();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }


  /**
   * Returns {@link ListBucketResultV1} of given bucket, marker, prefix and delimiter.
   *
   * @param bucketName Bucket name.
   * @param marker     Marker string.  List objects whose name is greater than `marker`.
   * @param prefix     Prefix string.  List objects whose name starts with `prefix`.
   * @param delimiter  delimiter string.  Group objects whose name contains `delimiter`.
   */
  private ListBucketResultV1 listObjectsV1(String bucketName, String marker, String prefix, String delimiter)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException {
    Map<String,String> queryParamMap = new HashMap<>();

    if (marker != null) {
      queryParamMap.put("marker", marker);
    }

    if (prefix != null) {
      queryParamMap.put("prefix", prefix);
    } else {
      queryParamMap.put("prefix", "");
    }

    if (delimiter != null) {
      queryParamMap.put("delimiter", delimiter);
    } else {
      queryParamMap.put("delimiter", "");
    }

    HttpResponse response = executeGet(bucketName, null, null, queryParamMap);

    ListBucketResultV1 result = new ListBucketResultV1();
    result.parseXml(response.body().charStream());
    response.body().close();
    return result;
  }


  /**
   * Returns all bucket information owned by the current user.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code List<Bucket> bucketList = minioClient.listBuckets();
   * for (Bucket bucket : bucketList) {
   *   System.out.println(bucket.creationDate() + ", " + bucket.name());
   * } }</pre>
   *
   * @return List of bucket type.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException   upon getting EOFException while reading given
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public List<Bucket> listBuckets()
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException {
    HttpResponse response = executeGet(null, null, null, null);
    ListAllMyBucketsResult result = new ListAllMyBucketsResult();
    result.parseXml(response.body().charStream());
    response.body().close();
    return result.buckets();
  }


  /**
   * Checks if given bucket exist and is having read access.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code boolean found = minioClient.bucketExists("my-bucketname");
   * if (found) {
   *   System.out.println("my-bucketname exists");
   * } else {
   *   System.out.println("my-bucketname does not exist");
   * } }</pre>
   *
   * @param bucketName Bucket name.
   *
   * @return True if the bucket exists and the user has at least read access.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException   upon getting EOFException while reading given
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidResponseException    upon a non-xml response from server
   *
   */
  public boolean bucketExists(String bucketName)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException {
    try {
      executeHead(bucketName, null);
      return true;
    } catch (ErrorResponseException e) {
      if (e.errorResponse().errorCode() != ErrorCode.NO_SUCH_BUCKET) {
        throw e;
      }
    }

    return false;
  }


  /**
   * Creates a bucket with default region.
   *
   * @param bucketName Bucket name.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws RegionConflictException  upon  passed region conflicts with the one
   *            previously specified
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution.
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InsufficientDataException   upon getting EOFException while reading given
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public void makeBucket(String bucketName)
    throws InvalidBucketNameException, RegionConflictException, NoSuchAlgorithmException, InsufficientDataException,
                IOException, InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
                InternalException, InvalidResponseException {
    this.makeBucket(bucketName, null, false);
  }


  /**
   * Creates a bucket with given region.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code minioClient.makeBucket("my-bucketname");
   * System.out.println("my-bucketname is created successfully"); }</pre>
   *
   * @param bucketName Bucket name.
   * @param region     region in which the bucket will be created.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws RegionConflictException     upon  passed region conflicts with the one
   *                                     previously specified.
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error

   * @throws InsufficientDataException   upon getting EOFException while reading given
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public void makeBucket(String bucketName, String region)
    throws InvalidBucketNameException, RegionConflictException, NoSuchAlgorithmException, InsufficientDataException,
                IOException, InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
                InternalException, InvalidResponseException {
    this.makeBucket(bucketName, region, false);
  }


  /**
   * Creates a bucket with given region and object lock option.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code minioClient.makeBucket("my-bucketname", "us-east-1", true);
   * System.out.println("my-bucketname is created successfully"); }</pre>
   *
   * @param bucketName Bucket name.
   * @param region     region in which the bucket will be created.
   * @param objectLock enable object lock support.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws RegionConflictException     upon  passed region conflicts with the one
   *                                     previously specified.
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error

   * @throws InsufficientDataException   upon getting EOFException while reading given
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public void makeBucket(String bucketName, String region, boolean objectLock)
    throws InvalidBucketNameException, RegionConflictException, NoSuchAlgorithmException, InsufficientDataException,
                IOException, InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
                InternalException, InvalidResponseException {
    // If region param is not provided, set it with the one provided by constructor
    if (region == null) {
      region = this.region;
    }
    // If constructor already sets a region, check if it is equal to region param if provided
    if (this.region != null && !this.region.equals(region)) {
      throw new RegionConflictException("passed region conflicts with the one previously specified");
    }
    String configString;
    if (region == null || US_EAST_1.equals(region)) {
      // for 'us-east-1', location constraint is not required.  for more info
      // http://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region
      region = US_EAST_1;
      configString = "";
    } else {
      CreateBucketConfiguration config = new CreateBucketConfiguration(region);
      configString = String.valueOf(config);
    }

    Map<String, String> headerMap = null;
    if (objectLock) {
      headerMap = new HashMap<>();
      headerMap.put("x-amz-bucket-object-lock-enabled", "true");
    }

    HttpResponse response = executePut(bucketName, null, headerMap, null, region, configString, 0);
    response.body().close();
  }


  /**
   * Enable object versioning in given bucket.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code minioClient.enableVersioning("my-bucketname");
   * System.out.println("object versioning is enabled in my-bucketname"); }</pre>
   *
   * @param bucketName Bucket name.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error

   * @throws InsufficientDataException   upon getting EOFException while reading given
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public void enableVersioning(String bucketName)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException,
                IOException, InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
                InternalException, InvalidResponseException {
    Map<String, String> queryParamMap = new HashMap<>();
    queryParamMap.put("versioning", "");
    String config = "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
        + "<Status>Enabled</Status></VersioningConfiguration>";
    HttpResponse response = executePut(bucketName, null, null, queryParamMap, config, 0);
    response.body().close();
  }


  /**
   * Disable object versioning in given bucket.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code minioClient.disableVersioning("my-bucketname");
   * System.out.println("object versioning is disabled in my-bucketname"); }</pre>
   *
   * @param bucketName Bucket name.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error

   * @throws InsufficientDataException   upon getting EOFException while reading given
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public void disableVersioning(String bucketName)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException,
                IOException, InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
                InternalException, InvalidResponseException {
    Map<String, String> queryParamMap = new HashMap<>();
    queryParamMap.put("versioning", "");
    String config = "<VersioningConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
        + "<Status>Suspended</Status></VersioningConfiguration>";
    HttpResponse response = executePut(bucketName, null, null, queryParamMap, config, 0);
    response.body().close();
  }


  /**
   * Sets default object retention in given bucket.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code minioClient.setDefaultRetention("my-bucketname", config);
   * System.out.println("Default object retention is set successfully in my-bucketname"); }</pre>
   *
   * @param bucketName Bucket name.
   * @param config     Object lock configuration.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public void setDefaultRetention(String bucketName, ObjectLockConfiguration config)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException {
    Map<String, String> queryParamMap = new HashMap<>();
    queryParamMap.put("object-lock", "");

    HttpResponse response = executePut(bucketName, null, null, queryParamMap, config, 0);
    response.body().close();
  }


  /**
   * Gets default object retention in given bucket.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code ObjectLockConfiguration config = minioClient.getDefaultRetention("my-bucketname"); }</pre>
   *
   * @param bucketName Bucket name.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public ObjectLockConfiguration getDefaultRetention(String bucketName)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException {
    Map<String, String> queryParamMap = new HashMap<>();
    queryParamMap.put("object-lock", "");

    HttpResponse response = executeGet(bucketName, null, null, queryParamMap);

    ObjectLockConfiguration result = new ObjectLockConfiguration();
    try {
      result.parseXml(response.body().charStream());
    } finally {
      response.body().close();
    }
    return result;
  }


  /**
   * Removes a bucket.
   * <p>
   * NOTE: -
   * All objects (including all object versions and delete markers) in the bucket
   * must be deleted prior, this API will not recursively delete objects
   * </p>
   *
   * </p><b>Example:</b><br>
   * <pre>{@code minioClient.removeBucket("my-bucketname");
   * System.out.println("my-bucketname is removed successfully"); }</pre>
   *
   * @param bucketName Bucket name.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InsufficientDataException   upon getting EOFException while reading given
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public void removeBucket(String bucketName)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException {
    executeDelete(bucketName, null, null);
  }

  /**
   * Uploads given file as object in given bucket.
   * <p>
   * If the object is larger than 5MB, the client will automatically use a multipart session.
   * </p>
   * <p>
   * If the session fails, the user may attempt to re-upload the object by attempting to create
   * the exact same object again.
   * </p>
   * <p>
   * If the multipart session fails, we abort all the uploaded content.
   * </p>
   *
   * @param bucketName  Bucket name.
   * @param objectName  Object name to create in the bucket.
   * @param fileName    File name to upload.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InsufficientDataException   upon getting EOFException while reading given
   * @throws InvalidResponseException    upon a non-xml response from server
   *
   * @deprecated As of release 7.0
   */
  @Deprecated
  public void putObject(String bucketName, String objectName, String fileName)
    throws InvalidBucketNameException, NoSuchAlgorithmException,  IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidArgumentException, InsufficientDataException, InvalidResponseException {
    putObject(bucketName, objectName, fileName, null, null, null, null);
  }

  /**
   * Uploads given file as object in given bucket.
   * <p>
   * If the object is larger than 5MB, the client will automatically use a multipart session.
   * </p>
   * <p>
   * If the session fails, the user may attempt to re-upload the object by attempting to create
   * the exact same object again.
   * </p>
   * <p>
   * If the multipart session fails, abort the uploaded parts automatically.
   * </p>
   *
   * @param bucketName  Bucket name.
   * @param objectName  Object name to create in the bucket.
   * @param fileName    File name to upload.
   * @param contentType File content type of the object, user supplied.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InsufficientDataException   upon getting EOFException while reading given
   * @throws InvalidResponseException    upon a non-xml response from server
   *
   * @deprecated As of release 7.0
   */
  @Deprecated
  public void putObject(String bucketName, String objectName, String fileName, String contentType)
    throws InvalidBucketNameException, NoSuchAlgorithmException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidArgumentException, InsufficientDataException, InvalidResponseException {
    putObject(bucketName, objectName, fileName, null, null, null, contentType );
  }

  /**
   * Uploads given file as object in given bucket and encrypt with a sse key.
   * <p>
   * If the object is larger than 5MB, the client will automatically use a multipart session.
   * </p>
   * <p>
   * If the session fails, the user may attempt to re-upload the object by attempting to create
   * the exact same object again.
   * </p>
   * <p>
   * If the multipart session fails, we abort the uploaded parts automatically.
   * </p>
   *
   * @param bucketName  Bucket name.
   * @param objectName  Object name to create in the bucket.
   * @param fileName    File name to upload.
   * @param sse         encryption metadata.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InsufficientDataException   upon getting EOFException while reading given
   * @throws InvalidResponseException    upon a non-xml response from server
   *
   * @deprecated As of release 7.0
   */
  @Deprecated
  public void putObject(String bucketName, String objectName, String fileName, ServerSideEncryption sse)
    throws InvalidBucketNameException, NoSuchAlgorithmException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidArgumentException, InsufficientDataException,InvalidResponseException {
    putObject(bucketName, objectName, fileName, null, null, sse, null);
  }


  /**
   * Uploads given file as object in given bucket.
   * <p>
   * If the object is larger than 5MB, the client will automatically use a multipart session.
   * </p>
   * <p>
   * If the session fails, the user may attempt to re-upload the object by attempting to create
   * the exact same object again.
   * </p>
   * <p>
   * If the multipart session fails, abort the uploaded parts automatically.
   * </p>
   *
   * @param bucketName  Bucket name.
   * @param objectName  Object name to create in the bucket.
   * @param fileName    File name to upload.
   * @param size        Size of all the data that will be uploaded.
   * @param headerMap   Custom/additional meta data of the object.
   * @param sse         encryption metadata.
   * @param contentType Content type of the stream.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InsufficientDataException   upon getting EOFException while reading given
   */
  public void putObject(String bucketName, String objectName, String fileName,  Long size,
                        Map<String, String> headerMap, ServerSideEncryption sse, String contentType)
    throws InvalidBucketNameException, NoSuchAlgorithmException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidArgumentException, InsufficientDataException, InvalidResponseException {

    if (fileName == null || "".equals(fileName)) {
      throw new InvalidArgumentException("empty file name is not allowed");
    }

    Path filePath = Paths.get(fileName);
    if (!Files.isRegularFile(filePath)) {
      throw new InvalidArgumentException("'" + fileName + "': not a regular file");
    }
    if (contentType == null) {
      contentType = Files.probeContentType(filePath);
    }
    if (size == null) {
      size = Files.size(filePath);
    }
    RandomAccessFile file = new RandomAccessFile(filePath.toFile(), "r");

    try {
      putObject(bucketName, objectName, size, file, headerMap, sse, contentType);
    } finally {
      file.close();
    }
  }


  /**
   * Uploads data from given stream as object to given bucket.
   * <p>
   * If the object is larger than 5MB, the client will automatically use a multipart session.
   * </p>
   * <p>
   * If the session fails, the user may attempt to re-upload the object by attempting to create
   * the exact same object again.
   * </p>
   * <p>
   * If the multipart session fails, we abort the uploaded parts automatically.
   * </p>
   *
   * </p><b>Example:</b><br>
   * <pre>{@code StringBuilder builder = new StringBuilder();
   * for (int i = 0; i < 1000; i++) {
   *   builder.append("Sphinx of black quartz, judge my vow: Used by Adobe InDesign to display font samples. ");
   *   builder.append("(29 letters)\n");
   *   builder.append("Jackdaws love my big sphinx of quartz: Similarly, used by Windows XP for some fonts. ");
   *   builder.append("(31 letters)\n");
   *   builder.append("Pack my box with five dozen liquor jugs: According to Wikipedia, this one is used on ");
   *   builder.append("NASAs Space Shuttle. (32 letters)\n");
   *   builder.append("The quick onyx goblin jumps over the lazy dwarf: Flavor text from an Unhinged Magic Card. ");
   *   builder.append("(39 letters)\n");
   *   builder.append("How razorback-jumping frogs can level six piqued gymnasts!: Not going to win any brevity ");
   *   builder.append("awards at 49 letters long, but old-time Mac users may recognize it.\n");
   *   builder.append("Cozy lummox gives smart squid who asks for job pen: A 41-letter tester sentence for Mac ");
   *   builder.append("computers after System 7.\n");
   *   builder.append("A few others we like: Amazingly few discotheques provide jukeboxes; Now fax quiz Jack! my ");
   *   builder.append("brave ghost pled; Watch Jeopardy!, Alex Trebeks fun TV quiz game.\n");
   *   builder.append("---\n");
   * }
   * ByteArrayInputStream bais = new ByteArrayInputStream(builder.toString().getBytes("UTF-8"));
   * // create object
   * minioClient.putObject("my-bucketname", "my-objectname", bais, bais.available(), "application/octet-stream");
   * bais.close();
   * System.out.println("my-objectname is uploaded successfully"); }</pre>
   *
   * @param bucketName  Bucket name.
   * @param objectName  Object name to create in the bucket.
   * @param stream      stream to upload.
   * @param size        Size of all the data that will be uploaded.
   * @param contentType Content type of the stream.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InsufficientDataException   upon getting EOFException while reading given
   * @throws InvalidResponseException    upon a non-xml response from server
   *
   * @deprecated As of release 7.0
   */
  @Deprecated
  public void putObject(String bucketName, String objectName, InputStream stream, long size, String contentType)
    throws InvalidBucketNameException, NoSuchAlgorithmException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidArgumentException, InsufficientDataException, InvalidResponseException {
    putObject(bucketName, objectName, stream, size, null, null, contentType);
  }

  /**
   * Uploads data from given stream as object to given bucket with specified meta data.
   * <p>
   * If the object is larger than 5MB, the client will automatically use a multipart session.
   * </p>
   * <p>
   * If the session fails, the user may attempt to re-upload the object by attempting to create
   * the exact same object again.
   * </p>
   * <p>
   * If the multipart session fails, we abort the uploaded parts automatically.
   * </p>
   *
   * </p><b>Example:</b><br>
   * <pre>{@code StringBuilder builder = new StringBuilder();
   * for (int i = 0; i < 1000; i++) {
   *   builder.append("Sphinx of black quartz, judge my vow: Used by Adobe InDesign to display font samples. ");
   *   builder.append("(29 letters)\n");
   *   builder.append("Jackdaws love my big sphinx of quartz: Similarly, used by Windows XP for some fonts. ");
   *   builder.append("(31 letters)\n");
   *   builder.append("Pack my box with five dozen liquor jugs: According to Wikipedia, this one is used on ");
   *   builder.append("NASAs Space Shuttle. (32 letters)\n");
   *   builder.append("The quick onyx goblin jumps over the lazy dwarf: Flavor text from an Unhinged Magic Card. ");
   *   builder.append("(39 letters)\n");
   *   builder.append("How razorback-jumping frogs can level six piqued gymnasts!: Not going to win any brevity ");
   *   builder.append("awards at 49 letters long, but old-time Mac users may recognize it.\n");
   *   builder.append("Cozy lummox gives smart squid who asks for job pen: A 41-letter tester sentence for Mac ");
   *   builder.append("computers after System 7.\n");
   *   builder.append("A few others we like: Amazingly few discotheques provide jukeboxes; Now fax quiz Jack! my ");
   *   builder.append("brave ghost pled; Watch Jeopardy!, Alex Trebeks fun TV quiz game.\n");
   *   builder.append("---\n");
   * }
   * ByteArrayInputStream bais = new ByteArrayInputStream(builder.toString().getBytes("UTF-8"));
   * // create object
   * Map<String, String> headerMap = new HashMap<>();
   * headerMap.put("Content-Type", "application/octet-stream");
   * minioClient.putObject("my-bucketname", "my-objectname", bais, headerMap);
   * bais.close();
   * System.out.println("my-objectname is uploaded successfully"); }</pre>
   *
   * @param bucketName  Bucket name.
   * @param objectName  Object name to create in the bucket.
   * @param stream      stream to upload.
   * @param headerMap   Custom/additional meta data of the object.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InsufficientDataException   upon getting EOFException while reading given
   * @throws InvalidResponseException    upon a non-xml response from server
   *
   * @deprecated As of release 7.0
   */
  @Deprecated
  public void putObject(String bucketName, String objectName, InputStream stream, Map<String, String> headerMap)
    throws InvalidBucketNameException, NoSuchAlgorithmException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidArgumentException, InsufficientDataException, InvalidResponseException {
    if (headerMap == null) {
      headerMap = new HashMap<>();
    }
    putObject(bucketName, objectName, stream, null, headerMap, null, null);
  }


  /**
   * Uploads data from given stream as object to given bucket with specified meta data.
   * <p>
   * If the object is larger than 5MB, the client will automatically use a multipart session.
   * </p>
   * <p>
   * If the session fails, the user may attempt to re-upload the object by attempting to create
   * the exact same object again.
   * </p>
   * <p>
   * If the multipart session fails, we abort the uploaded parts automatically.
   * </p>
   *
   * </p><b>Example:</b><br>
   * <pre>{@code StringBuilder builder = new StringBuilder();
   * for (int i = 0; i < 1000; i++) {
   *   builder.append("Sphinx of black quartz, judge my vow: Used by Adobe InDesign to display font samples. ");
   *   builder.append("(29 letters)\n");
   *   builder.append("Jackdaws love my big sphinx of quartz: Similarly, used by Windows XP for some fonts. ");
   *   builder.append("(31 letters)\n");
   *   builder.append("Pack my box with five dozen liquor jugs: According to Wikipedia, this one is used on ");
   *   builder.append("NASAs Space Shuttle. (32 letters)\n");
   *   builder.append("The quick onyx goblin jumps over the lazy dwarf: Flavor text from an Unhinged Magic Card. ");
   *   builder.append("(39 letters)\n");
   *   builder.append("How razorback-jumping frogs can level six piqued gymnasts!: Not going to win any brevity ");
   *   builder.append("awards at 49 letters long, but old-time Mac users may recognize it.\n");
   *   builder.append("Cozy lummox gives smart squid who asks for job pen: A 41-letter tester sentence for Mac ");
   *   builder.append("computers after System 7.\n");
   *   builder.append("A few others we like: Amazingly few discotheques provide jukeboxes; Now fax quiz Jack! my ");
   *   builder.append("brave ghost pled; Watch Jeopardy!, Alex Trebeks fun TV quiz game.\n");
   *   builder.append("---\n");
   * }
   * ByteArrayInputStream bais = new ByteArrayInputStream(builder.toString().getBytes("UTF-8"));
   * // create object
   * Map<String, String> headerMap = new HashMap<>();
   * headerMap.put("Content-Type", "application/octet-stream");
   * minioClient.putObject("my-bucketname", "my-objectname", bais, bais.available(), headerMap);
   * bais.close();
   * System.out.println("my-objectname is uploaded successfully"); }</pre>
   *
   * @param bucketName  Bucket name.
   * @param objectName  Object name to create in the bucket.
   * @param stream      stream to upload.
   * @param size        Size of all the data that will be uploaded.
   * @param headerMap   Custom/additional meta data of the object.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InsufficientDataException   upon getting EOFException while reading given
   * @throws InvalidResponseException    upon a non-xml response from server
   *
   * @deprecated As of release 7.0
   */
  @Deprecated
  public void putObject(String bucketName, String objectName, InputStream stream, long size,
                        Map<String, String> headerMap)
    throws InvalidBucketNameException, NoSuchAlgorithmException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidArgumentException, InsufficientDataException , InvalidResponseException {
    putObject(bucketName, objectName, stream, size, headerMap, null, null);
  }

  /**
   * Uploads data from given stream as object to given bucket where the stream size is unknown.
   * <p>
   * If the stream has more than 525MiB data, the client uses a multipart session automatically.
   * </p>
   * <p>
   * If the session fails, the user may attempt to re-upload the object by attempting to create
   * the exact same object again.
   * </p>
   * <p>
   * If the multipart session fails, we abort the uploaded parts automatically.
   * </p>
   *
   * </p><b>Example:</b><br>
   * <pre>{@code StringBuilder builder = new StringBuilder();
   * for (int i = 0; i < 1000; i++) {
   *   builder.append("Sphinx of black quartz, judge my vow: Used by Adobe InDesign to display font samples. ");
   *   builder.append("(29 letters)\n");
   *   builder.append("Jackdaws love my big sphinx of quartz: Similarly, used by Windows XP for some fonts. ");
   *   builder.append("(31 letters)\n");
   *   builder.append("Pack my box with five dozen liquor jugs: According to Wikipedia, this one is used on ");
   *   builder.append("NASAs Space Shuttle. (32 letters)\n");
   *   builder.append("The quick onyx goblin jumps over the lazy dwarf: Flavor text from an Unhinged Magic Card. ");
   *   builder.append("(39 letters)\n");
   *   builder.append("How razorback-jumping frogs can level six piqued gymnasts!: Not going to win any brevity ");
   *   builder.append("awards at 49 letters long, but old-time Mac users may recognize it.\n");
   *   builder.append("Cozy lummox gives smart squid who asks for job pen: A 41-letter tester sentence for Mac ");
   *   builder.append("computers after System 7.\n");
   *   builder.append("A few others we like: Amazingly few discotheques provide jukeboxes; Now fax quiz Jack! my ");
   *   builder.append("brave ghost pled; Watch Jeopardy!, Alex Trebeks fun TV quiz game.\n");
   *   builder.append("---\n");
   * }
   * ByteArrayInputStream bais = new ByteArrayInputStream(builder.toString().getBytes("UTF-8"));
   * // create object
   * minioClient.putObject("my-bucketname", "my-objectname", bais, "application/octet-stream");
   * bais.close();
   * System.out.println("my-objectname is uploaded successfully"); }</pre>
   *
   * @param bucketName  Bucket name.
   * @param objectName  Object name to create in the bucket.
   * @param stream      stream to upload.
   * @param sse         encryption metadata.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InsufficientDataException   upon getting EOFException while reading given
   * @throws InvalidResponseException    upon a non-xml response from server
   *
   * @deprecated As of release 7.0
   */
  @Deprecated
  public void putObject(String bucketName, String objectName, InputStream stream, long size,
                        ServerSideEncryption sse)
    throws InvalidBucketNameException, NoSuchAlgorithmException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidArgumentException, InsufficientDataException, InvalidResponseException {
    putObject(bucketName, objectName, stream, size, null, sse, null);
  }


    /**
     * Uploads data from given stream as object to given bucket with specified meta data
     * and encrypt the stream with a sse key.

     * <p>
     * If the object is larger than 5MB, the client will automatically use a multipart session.
     * </p>
     * <p>
     * If the session fails, the user may attempt to re-upload the object by attempting to create
     * the exact same object again.
     * </p>
     * <p>
     * If the multipart session fails, we abort the uploaded parts automatically.
     * </p>
     *
     * </p><b>Example:</b><br>
     * <pre>{@code StringBuilder builder = new StringBuilder();
     * for (int i = 0; i < 1000; i++) {
     *   builder.append("Sphinx of black quartz, judge my vow: Used by Adobe InDesign to display font samples. ");
     *   builder.append("(29 letters)\n");
     *   builder.append("Jackdaws love my big sphinx of quartz: Similarly, used by Windows XP for some fonts. ");
     *   builder.append("(31 letters)\n");
     *   builder.append("Pack my box with five dozen liquor jugs: According to Wikipedia, this one is used on ");
     *   builder.append("NASAs Space Shuttle. (32 letters)\n");
     *   builder.append("The quick onyx goblin jumps over the lazy dwarf: Flavor text from an Unhinged Magic Card. ");
     *   builder.append("(39 letters)\n");
     *   builder.append("How razorback-jumping frogs can level six piqued gymnasts!: Not going to win any brevity ");
     *   builder.append("awards at 49 letters long, but old-time Mac users may recognize it.\n");
     *   builder.append("Cozy lummox gives smart squid who asks for job pen: A 41-letter tester sentence for Mac ");
     *   builder.append("computers after System 7.\n");
     *   builder.append("A few others we like: Amazingly few discotheques provide jukeboxes; Now fax quiz Jack! my ");
     *   builder.append("brave ghost pled; Watch Jeopardy!, Alex Trebeks fun TV quiz game.\n");
     *   builder.append("---\n");
     * }
     * ByteArrayInputStream bais = new ByteArrayInputStream(builder.toString().getBytes("UTF-8"));

     * // create object
     * Map<String, String> headerMap = new HashMap<>();
     * headerMap.put("Content-Type", "application/octet-stream");
     * headerMap.put("X-Amz-Meta-Key", "meta-data");

     * //Generate symmetric 256 bit AES key.
     * KeyGenerator symKeyGenerator = KeyGenerator.getInstance("AES");
     * symKeyGenerator.init(256);
     * SecretKey symKey = symKeyGenerator.generateKey();

     * minioClient.putObject("my-bucketname", "my-objectname", bais, headerMap, symKey);
     * bais.close();
     * System.out.println("my-objectname is uploaded successfully"); }</pre>
     *
     * @param bucketName  Bucket name.
     * @param objectName  Object name to create in the bucket.
     * @param stream      stream to upload.
     * @param headerMap   Custom/additional meta data of the object.
     * @param sse         encryption metadata.
     *
     * @throws InvalidBucketNameException  upon invalid bucket name is given
     * @throws NoSuchAlgorithmException
     *           upon requested algorithm was not found during signature calculation
     * @throws IOException                 upon connection error
     * @throws InvalidKeyException
     *           upon an invalid access key or secret key
     * @throws NoResponseException         upon no response from server
     * @throws XmlPullParserException      upon parsing response xml
     * @throws ErrorResponseException      upon unsuccessful execution
     * @throws InternalException           upon internal library error
     * @throws InvalidArgumentException    upon invalid value is passed to a method.
     * @throws InsufficientDataException   upon getting EOFException while reading given
     * @throws InvalidResponseException    upon a non-xml response from server
     *
     * @deprecated As of release 7.0
     */
  @Deprecated
  public void putObject(String bucketName, String objectName, InputStream stream,
                          Map<String, String> headerMap, ServerSideEncryption sse)
    throws InvalidBucketNameException, NoSuchAlgorithmException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidArgumentException, InsufficientDataException, InvalidResponseException {
    putObject(bucketName, objectName, stream, null, headerMap, sse, null );
  }

    /**
   * Uploads data from given stream as object to given bucket with specified meta data
   * and encrypt the stream with a sse key.

   * <p>
   * If the object is larger than 5MB, the client will automatically use a multipart session.
   * </p>
   * <p>
   * If the session fails, the user may attempt to re-upload the object by attempting to create
   * the exact same object again.
   * </p>
   * <p>
   * If the multipart session fails, we abort the uploaded parts automatically.
   * </p>
   *
   * </p><b>Example:</b><br>
   * <pre>{@code StringBuilder builder = new StringBuilder();
   * for (int i = 0; i < 1000; i++) {
   *   builder.append("Sphinx of black quartz, judge my vow: Used by Adobe InDesign to display font samples. ");
   *   builder.append("(29 letters)\n");
   *   builder.append("Jackdaws love my big sphinx of quartz: Similarly, used by Windows XP for some fonts. ");
   *   builder.append("(31 letters)\n");
   *   builder.append("Pack my box with five dozen liquor jugs: According to Wikipedia, this one is used on ");
   *   builder.append("NASAs Space Shuttle. (32 letters)\n");
   *   builder.append("The quick onyx goblin jumps over the lazy dwarf: Flavor text from an Unhinged Magic Card. ");
   *   builder.append("(39 letters)\n");
   *   builder.append("How razorback-jumping frogs can level six piqued gymnasts!: Not going to win any brevity ");
   *   builder.append("awards at 49 letters long, but old-time Mac users may recognize it.\n");
   *   builder.append("Cozy lummox gives smart squid who asks for job pen: A 41-letter tester sentence for Mac ");
   *   builder.append("computers after System 7.\n");
   *   builder.append("A few others we like: Amazingly few discotheques provide jukeboxes; Now fax quiz Jack! my ");
   *   builder.append("brave ghost pled; Watch Jeopardy!, Alex Trebeks fun TV quiz game.\n");
   *   builder.append("---\n");
   * }
   * ByteArrayInputStream bais = new ByteArrayInputStream(builder.toString().getBytes("UTF-8"));

   * // create object
   * Map<String, String> headerMap = new HashMap<>();
   * headerMap.put("Content-Type", "application/octet-stream");
   * headerMap.put("X-Amz-Meta-Key", "meta-data");

   * //Generate symmetric 256 bit AES key.
   * KeyGenerator symKeyGenerator = KeyGenerator.getInstance("AES");
   * symKeyGenerator.init(256);
   * SecretKey symKey = symKeyGenerator.generateKey();

   * minioClient.putObject("my-bucketname", "my-objectname", bais, bais.available(), headerMap, symKey);
   * bais.close();
   * System.out.println("my-objectname is uploaded successfully"); }</pre>
   *
   * @param bucketName  Bucket name.
   * @param objectName  Object name to create in the bucket.
   * @param stream      stream to upload.
   * @param headerMap   Custom/additional meta data of the object.
   * @param sse         encryption metadata.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InsufficientDataException   upon getting EOFException while reading given
   * @throws InvalidResponseException    upon a non-xml response from server
   *
   * @deprecated As of release 7.0
   */
  @Deprecated
  public void putObject(String bucketName, String objectName, InputStream stream, long size,
                        Map<String, String> headerMap, ServerSideEncryption sse)
    throws InvalidBucketNameException, NoSuchAlgorithmException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidArgumentException, InsufficientDataException, InvalidResponseException {
    putObject(bucketName, objectName, stream, size, headerMap, sse, null);
  }

  /**
   * Uploads data from given stream as object to given bucket where the stream size is unknown.
   * <p>
   * If the stream has more than 525MiB data, the client uses a multipart session automatically.
   * </p>
   * <p>
   * If the session fails, the user may attempt to re-upload the object by attempting to create
   * the exact same object again.
   * </p>
   * <p>
   * If the multipart session fails, we abort the uploaded parts automaticlly.
   * </p>
   *
   * </p><b>Example:</b><br>
   * <pre>{@code StringBuilder builder = new StringBuilder();
   * for (int i = 0; i < 1000; i++) {
   *   builder.append("Sphinx of black quartz, judge my vow: Used by Adobe InDesign to display font samples. ");
   *   builder.append("(29 letters)\n");
   *   builder.append("Jackdaws love my big sphinx of quartz: Similarly, used by Windows XP for some fonts. ");
   *   builder.append("(31 letters)\n");
   *   builder.append("Pack my box with five dozen liquor jugs: According to Wikipedia, this one is used on ");
   *   builder.append("NASAs Space Shuttle. (32 letters)\n");
   *   builder.append("The quick onyx goblin jumps over the lazy dwarf: Flavor text from an Unhinged Magic Card. ");
   *   builder.append("(39 letters)\n");
   *   builder.append("How razorback-jumping frogs can level six piqued gymnasts!: Not going to win any brevity ");
   *   builder.append("awards at 49 letters long, but old-time Mac users may recognize it.\n");
   *   builder.append("Cozy lummox gives smart squid who asks for job pen: A 41-letter tester sentence for Mac ");
   *   builder.append("computers after System 7.\n");
   *   builder.append("A few others we like: Amazingly few discotheques provide jukeboxes; Now fax quiz Jack! my ");
   *   builder.append("brave ghost pled; Watch Jeopardy!, Alex Trebeks fun TV quiz game.\n");
   *   builder.append("---\n");
   * }
   * ByteArrayInputStream bais = new ByteArrayInputStream(builder.toString().getBytes("UTF-8"));
   * // create object
   * minioClient.putObject("my-bucketname", "my-objectname", bais, "application/octet-stream");
   * bais.close();
   * System.out.println("my-objectname is uploaded successfully"); }</pre>
   *
   * @param bucketName  Bucket name.
   * @param objectName  Object name to create in the bucket.
   * @param stream      stream to upload.
   * @param contentType Content type of the stream.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InvalidResponseException    upon a non-xml response from server
   *
   * @deprecated As of release 7.0
   */
  @Deprecated
  public void putObject(String bucketName, String objectName, InputStream stream, String contentType)
    throws InvalidBucketNameException, NoSuchAlgorithmException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidArgumentException, InsufficientDataException, InvalidResponseException {
    putObject(bucketName, objectName, stream, null, null, null, contentType);
  }


  /**
   * Uploads data from given stream as object to given bucket where the stream size is unknown.
   * <p>
   * If the stream has more than 525MiB data, the client uses a multipart session automatically.
   * </p>
   * <p>
   * If the session fails, the user may attempt to re-upload the object by attempting to create
   * the exact same object again.
   * </p>
   * <p>
   * If the multipart session fails, we abort the uploaded parts automaticlly.
   * </p>
   *
   * </p><b>Example:</b><br>
   * <pre>{@code StringBuilder builder = new StringBuilder();
   * for (int i = 0; i < 1000; i++) {
   *   builder.append("Sphinx of black quartz, judge my vow: Used by Adobe InDesign to display font samples. ");
   *   builder.append("(29 letters)\n");
   *   builder.append("Jackdaws love my big sphinx of quartz: Similarly, used by Windows XP for some fonts. ");
   *   builder.append("(31 letters)\n"); 
   *   builder.append("Pack my box with five dozen liquor jugs: According to Wikipedia, this one is used on ");
   *   builder.append("NASAs Space Shuttle. (32 letters)\n");
   *   builder.append("The quick onyx goblin jumps over the lazy dwarf: Flavor text from an Unhinged Magic Card. ");
   *   builder.append("(39 letters)\n");
   *   builder.append("How razorback-jumping frogs can level six piqued gymnasts!: Not going to win any brevity ");
   *   builder.append("awards at 49 letters long, but old-time Mac users may recognize it.\n");
   *   builder.append("Cozy lummox gives smart squid who asks for job pen: A 41-letter tester sentence for Mac ");
   *   builder.append("computers after System 7.\n");
   *   builder.append("A few others we like: Amazingly few discotheques provide jukeboxes; Now fax quiz Jack! my ");
   *   builder.append("brave ghost pled; Watch Jeopardy!, Alex Trebeks fun TV quiz game.\n");
   *   builder.append("---\n");
   * }
   * ByteArrayInputStream bais = new ByteArrayInputStream(builder.toString().getBytes("UTF-8"));
   * // create object
   * Map<String, String> headerMap = new HashMap<>();
   * headerMap.put("Content-Type", "application/octet-stream");
   * headerMap.put("X-Amz-Meta-Key", "meta-data");

   * //Generate symmetric 256 bit AES key.
   * KeyGenerator symKeyGenerator = KeyGenerator.getInstance("AES");
   * symKeyGenerator.init(256);
   * SecretKey symKey = symKeyGenerator.generateKey();

   * minioClient.putObject("my-bucketname", "my-objectname", bais, bais.available(), headerMap, symKey);
   * bais.close();
   * System.out.println("my-objectname is uploaded successfully"); }</pre>
   *
   * @param bucketName  Bucket name.
   * @param objectName  Object name to create in the bucket.
   * @param stream      stream to upload.
   * @param size        Size of all the data that will be uploaded.
   * @param headerMap   Custom/additional meta data of the object.
   * @param sse         encryption metadata.
   * @param contentType Content type of the stream.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InvalidResponseException    upon a non-xml response from server
//   * @see #putObject(String bucketName, String objectName, String fileName)
   */
  public void putObject(String bucketName, String objectName, InputStream stream, Long size,
                        Map<String, String> headerMap, ServerSideEncryption sse, String contentType)
    throws InvalidBucketNameException, NoSuchAlgorithmException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidArgumentException, InsufficientDataException, InvalidResponseException {

    if (!(stream instanceof BufferedInputStream)) {
      stream = new BufferedInputStream(stream);
    }
    putObject(bucketName, objectName, size, stream, headerMap, sse, contentType);
  }

  /**
   * Executes put object and returns ETag of the object.
   *
   * @param bucketName
   *          Bucket name.
   * @param objectName
   *          Object name in the bucket.
   * @param length
   *          Length of object data.
   * @param data
   *          Object data.
   * @param uploadId
   *          Upload ID of multipart put object.
   * @param partNumber
   *          Part number of multipart put object.
   */
  private String putObject(String bucketName, String objectName, Object data, int length,
                           Map<String, String> headerMap, String uploadId, int partNumber)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException {
    HttpResponse response = null;

    Map<String,String> queryParamMap = null;
    if (partNumber > 0 && uploadId != null && !"".equals(uploadId)) {
      queryParamMap = new HashMap<>();
      queryParamMap.put("partNumber", Integer.toString(partNumber));
      queryParamMap.put(UPLOAD_ID, uploadId);
    }

    response = executePut(bucketName, objectName, headerMap, queryParamMap, data, length);

    response.body().close();
    return response.header().etag();
  }


  /**
   * Executes put object. If size of object data is <= 5MiB, single put object is used
   * else multipart put object is used.
   *
   * @param bucketName
   *          Bucket name.
   * @param objectName
   *          Object name in the bucket.
   * @param size
   *          Size of object data.
   * @param data
   *          Object data.
   */
  private void putObject(String bucketName, String objectName, Long size, Object data,
      Map<String, String> headerMap, ServerSideEncryption sse,  String contentType)
    throws InvalidBucketNameException, NoSuchAlgorithmException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidArgumentException, InsufficientDataException, InvalidResponseException {
    boolean unknownSize = false;

    if (size == null) {
      unknownSize = true;
      size = MAX_OBJECT_SIZE;
    }

    if (headerMap == null) {
      headerMap = new HashMap<>();
    }
    // Add content type if not already set
    // Content Type being passed as argument has higher precedence
    // than the one passed in headerMap if any.
    if (contentType == null) {
      if (headerMap.get("Content-Type") == null) {
        headerMap.put("Content-Type", "application/octet-stream");
      }
    } else {
      headerMap.put("Content-Type", contentType);
    }

    if (sse != null) {
      checkWriteRequestSse(sse);
      headerMap.putAll(sse.headers());
    }


    if (size <= MIN_MULTIPART_SIZE) {
      putObject(bucketName, objectName, data, size.intValue(), headerMap, null, 0);
      return;
    }

    /* Multipart upload */
    int[] rv = calculateMultipartSize(size);
    int partSize = rv[0];
    int partCount = rv[1];
    int lastPartSize = rv[2];
    Part[] totalParts = new Part[partCount];

    // initiate new multipart upload.
    String uploadId = initMultipartUpload(bucketName, objectName, headerMap);

    try {
      int expectedReadSize = partSize;
      for (int partNumber = 1; partNumber <= partCount; partNumber++) {
        if (partNumber == partCount) {
          expectedReadSize = lastPartSize;
        }

        // For unknown sized stream, check available size.
        int availableSize = 0;
        if (unknownSize) {
          // Check whether data is available one byte more than expectedReadSize.
          availableSize = getAvailableSize(data, expectedReadSize + 1);
          // If availableSize is less or equal to expectedReadSize, then we reached last part.
          if (availableSize <= expectedReadSize) {
            // If it is first part, do single put object.
            if (partNumber == 1) {
              putObject(bucketName, objectName, data, availableSize, headerMap, null, 0);
              return;
            }
            expectedReadSize = availableSize;
            partCount = partNumber;
          }
        }

        Map<String, String> encryptionHeaders = null;
        // In multi-part uploads, set encryption headers in the case of SSE-C.
        if (sse != null && sse.type() == ServerSideEncryption.Type.SSE_C) {
          encryptionHeaders = sse.headers();
        }

        String etag = putObject(bucketName, objectName, data, expectedReadSize, encryptionHeaders,
                                uploadId, partNumber);
        totalParts[partNumber - 1] = new Part(partNumber, etag);
      }
      // All parts have been uploaded, complete the multipart upload.
      completeMultipart(bucketName, objectName, uploadId, totalParts);
    } catch (RuntimeException e) {
      abortMultipartUpload(bucketName, objectName, uploadId);
      throw e;
    } catch (Exception e) {
      abortMultipartUpload(bucketName, objectName, uploadId);
      throw e;
    }
  }

  /**
   * Get JSON string of bucket policy of the given bucket.
   *
   * @param bucketName the name of the bucket for which policies are to be listed.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code
   *
   * String policyString = minioClient.getBucketPolicy("my-bucketname");
   * }</pre>
   *
   * @return bucket policy JSON string.
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws InvalidObjectPrefixException upon invalid object prefix.
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws BucketPolicyTooLargeException  upon bucket policy too large in size
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public String getBucketPolicy(String bucketName)
    throws InvalidBucketNameException, InvalidObjectPrefixException, NoSuchAlgorithmException,
           InsufficientDataException, IOException, InvalidKeyException, NoResponseException,
           XmlPullParserException, ErrorResponseException, InternalException, BucketPolicyTooLargeException,
           InvalidResponseException {
    Map<String,String> queryParamMap = new HashMap<>();
    queryParamMap.put("policy", "");

    HttpResponse response = null;
    byte[] buf = new byte[MAX_BUCKET_POLICY_SIZE];
    int bytesRead = 0;

    try {
      response = executeGet(bucketName, null, null, queryParamMap);
      bytesRead = response.body().byteStream().read(buf, 0, MAX_BUCKET_POLICY_SIZE);
      if (bytesRead < 0) {
        // reached EOF
        throw new IOException("reached EOF when reading bucket policy");
      }

      // Read one byte extra to ensure only MAX_BUCKET_POLICY_SIZE data is sent by the server.
      if (bytesRead == MAX_BUCKET_POLICY_SIZE) {
        int byteRead = 0;
        while (byteRead == 0) {
          byteRead = response.body().byteStream().read();
          if (byteRead < 0) {
            // reached EOF which is fine.
            break;
          } else if (byteRead > 0) {
            throw new BucketPolicyTooLargeException(bucketName);
          }
        }
      }
    } catch (ErrorResponseException e) {
      if (e.errorResponse().errorCode() != ErrorCode.NO_SUCH_BUCKET_POLICY) {
        throw e;
      }
    } finally {
      if (response != null && response.body() != null) {
        response.body().close();
      }
    }

    return new String(buf, 0, bytesRead, StandardCharsets.UTF_8);
  }


  /**
   * Set JSON string of policy on given bucket.
   *
   * @param bucketName   Bucket name.
   * @param policy       Bucket policy JSON string.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code StringBuilder builder = new StringBuilder();
   * builder.append("{\n");
   * builder.append("    \"Statement\": [\n");
   * builder.append("        {\n");
   * builder.append("            \"Action\": [\n");
   * builder.append("                \"s3:GetBucketLocation\",\n");
   * builder.append("                \"s3:ListBucket\"\n");
   * builder.append("            ],\n");
   * builder.append("            \"Effect\": \"Allow\",\n");
   * builder.append("            \"Principal\": \"*\",\n");
   * builder.append("            \"Resource\": \"arn:aws:s3:::my-bucketname\"\n");
   * builder.append("        },\n");
   * builder.append("        {\n");
   * builder.append("            \"Action\": \"s3:GetObject\",\n");
   * builder.append("            \"Effect\": \"Allow\",\n");
   * builder.append("            \"Principal\": \"*\",\n");
   * builder.append("            \"Resource\": \"arn:aws:s3:::my-bucketname/myobject*\"\n");
   * builder.append("        }\n");
   * builder.append("    ],\n");
   * builder.append("    \"Version\": \"2012-10-17\"\n");
   * builder.append("}\n");
   * setBucketPolicy("my-bucketname", builder.toString()); }</pre>
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws InvalidObjectPrefixException upon invalid object prefix.
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public void setBucketPolicy(String bucketName, String policy)
    throws InvalidBucketNameException, InvalidObjectPrefixException, NoSuchAlgorithmException,
           InsufficientDataException, IOException, InvalidKeyException, NoResponseException,
           XmlPullParserException, ErrorResponseException, InternalException, InvalidResponseException {
    Map<String,String> headerMap = new HashMap<>();
    headerMap.put("Content-Type", "application/json");

    Map<String,String> queryParamMap = new HashMap<>();
    queryParamMap.put("policy", "");

    HttpResponse response = executePut(bucketName, null, headerMap, queryParamMap, policy, 0);
    response.body().close();
  }

  /**
   * Set XML string of LifeCycle on a given bucket.
   * Delete the lifecycle of bucket in case a null is passed as lifeCycle.
   * @param bucketName   Bucket name.
   * @param lifeCycle    Bucket policy XML string.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code String lifecycle  = "<LifecycleConfiguration><Rule><ID>expire-bucket</ID><Prefix></Prefix>"
   * + "<Status>Enabled</Status><Expiration><Days>365</Days></Expiration></Rule></LifecycleConfiguration>";
   *
   * setBucketLifecycle("my-bucketname", lifecycle); }</pre>
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException    upon requested algorithm was not found during
   *                                     signature calculation
   * @throws InsufficientDataException   upon getting EOFException while reading given
   *                                     InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException         upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidArgumentException    upon invalid value is passed to a method.
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public void setBucketLifeCycle(String bucketName, String lifeCycle)
          throws InvalidBucketNameException, NoSuchAlgorithmException,
          InsufficientDataException, IOException, InvalidKeyException, NoResponseException,
          XmlPullParserException, ErrorResponseException, InternalException,InvalidArgumentException,
          InvalidResponseException {
    if ((lifeCycle == null) || "".equals(lifeCycle)) {
      throw new InvalidArgumentException("life cycle cannot be empty");
    }
    Map<String, String> headerMap = new HashMap<>();
    headerMap.put("Content-Length", Integer.toString(lifeCycle.length()));
    Map<String, String> queryParamMap = new HashMap<>();
    queryParamMap.put("lifecycle", "");
    HttpResponse response = executePut(bucketName, null, headerMap, queryParamMap, lifeCycle, 0);
    response.body().close();
  }

  /**
   * Delete the LifeCycle of bucket.
   *
   * @param bucketName   Bucket name.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code  deleteBucketLifeCycle("my-bucketname"); }</pre>
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException    upon requested algorithm was not found during
   *                                     signature calculation
   * @throws InsufficientDataException   upon getting EOFException while reading given
   *                                     InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException         upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public void deleteBucketLifeCycle(String bucketName)
          throws InvalidBucketNameException, NoSuchAlgorithmException,
          InsufficientDataException, IOException, InvalidKeyException, NoResponseException,
          XmlPullParserException, ErrorResponseException, InternalException, InvalidResponseException {
    Map<String,String> queryParamMap = new HashMap<>();
    queryParamMap.put("lifecycle", "");
    HttpResponse response = executeDelete(bucketName,  "", queryParamMap);
    response.body().close();
  }

  /** Get bucket life cycle configuration.
   *
   * @param bucketName   Bucket name.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code String bucketLifeCycle = minioClient.getBucketLifecycle("my-bucketname");
   * }</pre>
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException    upon requested algorithm was not found during
   *                                     signature calculation
   * @throws InsufficientDataException   upon getting EOFException while reading given
   *                                     InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException         upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidResponseException    upon a non-xml response from server
   *
   */
  public String getBucketLifeCycle(String bucketName)
          throws InvalidBucketNameException, NoSuchAlgorithmException,
          InsufficientDataException, IOException, InvalidKeyException, NoResponseException,
          XmlPullParserException, ErrorResponseException, InternalException, InvalidResponseException {
    Map<String,String> queryParamMap = new HashMap<>();
    queryParamMap.put("lifecycle", "");
    HttpResponse response = null;
    String bodyContent = "";
    Scanner scanner = null ;
    try {
      response = executeGet(bucketName, "", null, queryParamMap);
      scanner = new Scanner(response.body().charStream());
      // read entire body stream to string.
      scanner.useDelimiter("\\A");
      if (scanner.hasNext()) {
        bodyContent = scanner.next();
      }
    } catch (ErrorResponseException e) {
      if (e.errorResponse().errorCode() != ErrorCode.NO_SUCH_LIFECYCLE_CONFIGURATION) {
        throw e;
      }
    } finally {
      if (response != null && response.body() != null) {
        response.body().close();
      }
      if (scanner != null) {
        scanner.close();
      }
    }
    return bodyContent;
  }

  /**
   * Get bucket notification configuration
   *
   * @param bucketName   Bucket name.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code NotificationConfiguration notificationConfig = minioClient.getBucketNotification("my-bucketname");
   * }</pre>
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws InvalidObjectPrefixException upon invalid object prefix.
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidResponseException    upon a non-xml response from server
   *
   */
  public NotificationConfiguration getBucketNotification(String bucketName)
    throws InvalidBucketNameException, InvalidObjectPrefixException, NoSuchAlgorithmException,
           InsufficientDataException, IOException, InvalidKeyException, NoResponseException,
           XmlPullParserException, ErrorResponseException, InternalException, InvalidResponseException {
    Map<String,String> queryParamMap = new HashMap<>();
    queryParamMap.put("notification", "");

    HttpResponse response = executeGet(bucketName, null, null, queryParamMap);
    NotificationConfiguration result = new NotificationConfiguration();
    try {
      result.parseXml(response.body().charStream());
    } finally {
      response.body().close();
    }

    return result;
  }


  /**
   * Set bucket notification configuration
   *
   * @param bucketName   Bucket name.
   * @param notificationConfiguration   Notification configuration to be set.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code minioClient.setBucketNotification("my-bucketname", notificationConfiguration);
   * }</pre>
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws InvalidObjectPrefixException upon invalid object prefix.
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidResponseException    upon a non-xml response from server
   *
   */
  public void setBucketNotification(String bucketName, NotificationConfiguration notificationConfiguration)
    throws InvalidBucketNameException, InvalidObjectPrefixException, NoSuchAlgorithmException,
           InsufficientDataException, IOException, InvalidKeyException, NoResponseException,
           XmlPullParserException, ErrorResponseException, InternalException, InvalidResponseException {
    Map<String,String> queryParamMap = new HashMap<>();
    queryParamMap.put("notification", "");
    HttpResponse response = executePut(bucketName, null, null, queryParamMap, notificationConfiguration.toString(), 0);
    response.body().close();
  }


  /**
   * Remove all bucket notification.
   *
   * @param bucketName   Bucket name.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code minioClient.removeAllBucketNotification("my-bucketname");
   * }</pre>
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws InvalidObjectPrefixException upon invalid object prefix.
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public void removeAllBucketNotification(String bucketName)
    throws InvalidBucketNameException, InvalidObjectPrefixException, NoSuchAlgorithmException,
           InsufficientDataException, IOException, InvalidKeyException, NoResponseException,
           XmlPullParserException, ErrorResponseException, InternalException, InvalidResponseException {
    NotificationConfiguration notificationConfiguration = new NotificationConfiguration();
    setBucketNotification(bucketName, notificationConfiguration);
  }

  /**
   * Lists incomplete uploads of objects in given bucket.
   *
   * @param bucketName Bucket name.
   *
   * @return an iterator of Upload.
//   * @see #listIncompleteUploads(String, String, boolean)
   */
  public Iterable<Result<Upload>> listIncompleteUploads(String bucketName) throws XmlPullParserException {
    return listIncompleteUploads(bucketName, null, true, true);
  }

  /**
   * Lists incomplete uploads of objects in given bucket and prefix.
   *
   * @param bucketName Bucket name.
   * @param prefix filters the list of uploads to include only those that start with prefix.
   *
   * @return an iterator of Upload.
   *
//   * @throws XmlPullParserException      upon parsing response xml
//   * @see #listIncompleteUploads(String, String, boolean)
   */
  public Iterable<Result<Upload>> listIncompleteUploads(String bucketName, String prefix)
          throws XmlPullParserException {
    return listIncompleteUploads(bucketName, prefix, true, true);
  }

  /**
   * Lists incomplete uploads of objects in given bucket, prefix and recursive flag.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code Iterable<Result<Upload>> myObjects = minioClient.listIncompleteUploads("my-bucketname");
   * for (Result<Upload> result : myObjects) {
   *   Upload upload = result.get();
   *   System.out.println(upload.uploadId() + ", " + upload.objectName());
   * } }</pre>
   *
   * @param bucketName  Bucket name.
   * @param prefix      Prefix string.  List objects whose name starts with `prefix`.
   * @param recursive when false, emulates a directory structure where each listing returned is either a full object
   *                  or part of the object's key up to the first '/'. All uploads with the same prefix up to the first
   *                  '/' will be merged into one entry.
   *
   * @return an iterator of Upload.
   *
//   * @see #listIncompleteUploads(String bucketName)
//   * @see #listIncompleteUploads(String bucketName, String prefix)
   */
  public Iterable<Result<Upload>> listIncompleteUploads(String bucketName, String prefix, boolean recursive) {
    return listIncompleteUploads(bucketName, prefix, recursive, true);
  }

  /**
   * Returns {@code Iterable<Result<Upload>>} of given bucket name, prefix and recursive flag.
   * All parts size are aggregated when aggregatePartSize is true.
   */
  private Iterable<Result<Upload>> listIncompleteUploads(final String bucketName, final String prefix,
                                                         final boolean recursive, final boolean aggregatePartSize) {
    return new Iterable<Result<Upload>>() {
      @Override
      public Iterator<Result<Upload>> iterator() {
        return new Iterator<Result<Upload>>() {
          private String nextKeyMarker;
          private String nextUploadIdMarker;
          private ListMultipartUploadsResult listMultipartUploadsResult;
          private Result<Upload> error;
          private Iterator<Upload> uploadIterator;
          private boolean completed = false;

          private synchronized void populate() {
            String delimiter = "/";
            if (recursive) {
              delimiter = null;
            }

            this.listMultipartUploadsResult = null;
            this.uploadIterator = null;

            try {
              this.listMultipartUploadsResult = listIncompleteUploads(bucketName, nextKeyMarker, nextUploadIdMarker,
                                                                      prefix, delimiter, 1000);
            } catch (InvalidBucketNameException | NoSuchAlgorithmException | InsufficientDataException | IOException
                     | InvalidKeyException | NoResponseException | XmlPullParserException | ErrorResponseException
                     | InternalException | InvalidResponseException e) {
              this.error = new Result<>(null, e);
            } finally {
              if (this.listMultipartUploadsResult != null) {
                this.uploadIterator = this.listMultipartUploadsResult.uploads().iterator();
              } else {
                this.uploadIterator = new LinkedList<Upload>().iterator();
              }
            }
          }

          private synchronized long getAggregatedPartSize(String objectName, String uploadId)
            throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
                   InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
                   InternalException {
            long aggregatedPartSize = 0;

            for (Result<Part> result : listObjectParts(bucketName, objectName, uploadId)) {
              aggregatedPartSize += result.get().partSize();
            }

            return aggregatedPartSize;
          }

          @Override
          public boolean hasNext() {
            if (this.completed) {
              return false;
            }

            if (this.error == null && this.uploadIterator == null) {
              populate();
            }

            if (this.error == null && !this.uploadIterator.hasNext()
                  && this.listMultipartUploadsResult.isTruncated()) {
              this.nextKeyMarker = this.listMultipartUploadsResult.nextKeyMarker();
              this.nextUploadIdMarker = this.listMultipartUploadsResult.nextUploadIdMarker();
              populate();
            }

            if (this.error != null) {
              return true;
            }

            if (this.uploadIterator.hasNext()) {
              return true;
            }

            this.completed = true;
            return false;
          }

          @Override
          public Result<Upload> next() {
            if (this.completed) {
              throw new NoSuchElementException();
            }

            if (this.error == null && this.uploadIterator == null) {
              populate();
            }

            if (this.error == null && !this.uploadIterator.hasNext()
                  && this.listMultipartUploadsResult.isTruncated()) {
              this.nextKeyMarker = this.listMultipartUploadsResult.nextKeyMarker();
              this.nextUploadIdMarker = this.listMultipartUploadsResult.nextUploadIdMarker();
              populate();
            }

            if (this.error != null) {
              this.completed = true;
              return this.error;
            }

            if (this.uploadIterator.hasNext()) {
              Upload upload = this.uploadIterator.next();

              if (aggregatePartSize) {
                long aggregatedPartSize;

                try {
                  aggregatedPartSize = getAggregatedPartSize(upload.objectName(), upload.uploadId());
                } catch (InvalidBucketNameException | NoSuchAlgorithmException | InsufficientDataException | IOException
                         | InvalidKeyException | NoResponseException | XmlPullParserException | ErrorResponseException
                         | InternalException e) {
                  // special case: ignore the error as we can't propagate the exception in next()
                  aggregatedPartSize = -1;
                }

                upload.setAggregatedPartSize(aggregatedPartSize);
              }

              return new Result<>(upload, null);
            }

            this.completed = true;
            throw new NoSuchElementException();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }

  /**
   * Executes List Incomplete uploads S3 call for given bucket name, key marker, upload id marker, prefix,
   * delimiter and maxUploads and returns {@link ListMultipartUploadsResult}.
   */
  private ListMultipartUploadsResult listIncompleteUploads(String bucketName, String keyMarker, String uploadIdMarker,
                                                           String prefix, String delimiter, int maxUploads)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException {
    if (maxUploads < 0 || maxUploads > 1000) {
      maxUploads = 1000;
    }

    Map<String,String> queryParamMap = new HashMap<>();
    queryParamMap.put("uploads", "");
    queryParamMap.put("max-uploads", Integer.toString(maxUploads));

    if (prefix != null) {
      queryParamMap.put("prefix", prefix);
    } else {
      queryParamMap.put("prefix", "");
    }

    if (delimiter != null) {
      queryParamMap.put("delimiter", delimiter);
    } else {
      queryParamMap.put("delimiter", "");
    }

    if (keyMarker != null) {
      queryParamMap.put("key-marker", keyMarker);
    }

    if (uploadIdMarker != null) {
      queryParamMap.put("upload-id-marker", uploadIdMarker);
    }

    HttpResponse response = executeGet(bucketName, null, null, queryParamMap);

    ListMultipartUploadsResult result = new ListMultipartUploadsResult();
    result.parseXml(response.body().charStream());
    response.body().close();
    return result;
  }


  /**
   * Initializes new multipart upload for given bucket name, object name and content type.
   */
  private String initMultipartUpload(String bucketName, String objectName, Map<String, String> headerMap)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException , InvalidResponseException {
    // set content type if not set already
    if ((headerMap != null) && (headerMap.get("Content-Type") == null)) {
      headerMap.put("Content-Type", "application/octet-stream");
    }

    Map<String,String> queryParamMap = new HashMap<>();
    queryParamMap.put("uploads", "");

    HttpResponse response = executePost(bucketName, objectName, headerMap, queryParamMap, "");

    InitiateMultipartUploadResult result = new InitiateMultipartUploadResult();
    result.parseXml(response.body().charStream());
    response.body().close();
    return result.uploadId();
  }


  /**
   * Executes complete multipart upload of given bucket name, object name, upload ID and parts.
   */
  private void completeMultipart(String bucketName, String objectName, String uploadId, Part[] parts)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException {
    Map<String,String> queryParamMap = new HashMap<>();
    queryParamMap.put(UPLOAD_ID, uploadId);

    CompleteMultipartUpload completeManifest = new CompleteMultipartUpload(parts);

    HttpResponse response = executePost(bucketName, objectName, null, queryParamMap, completeManifest);

    // Fixing issue https://github.com/minio/minio-java/issues/391
    String bodyContent = "";
    Scanner scanner = new Scanner(response.body().charStream());
    try {
      // read entire body stream to string.
      scanner.useDelimiter("\\A");
      if (scanner.hasNext()) {
        bodyContent = scanner.next();
      }
    } finally {
      response.body().close();
      scanner.close();
    }

    bodyContent = bodyContent.trim();
    if (!bodyContent.isEmpty()) {
      ErrorResponse errorResponse = new ErrorResponse(new StringReader(bodyContent));
      if (errorResponse.code() != null) {
        throw new ErrorResponseException(errorResponse, response.response());
      }
    }
  }


  /**
   * Executes List object parts of multipart upload for given bucket name, object name and upload ID and
   * returns {@code Iterable<Result<Part>>}.
   */
  private Iterable<Result<Part>> listObjectParts(final String bucketName, final String objectName,
                                                 final String uploadId) {
    return new Iterable<Result<Part>>() {
      @Override
      public Iterator<Result<Part>> iterator() {
        return new Iterator<Result<Part>>() {
          private int nextPartNumberMarker;
          private ListPartsResult listPartsResult;
          private Result<Part> error;
          private Iterator<Part> partIterator;
          private boolean completed = false;

          private synchronized void populate() {
            this.listPartsResult = null;
            this.partIterator = null;

            try {
              this.listPartsResult = listObjectParts(bucketName, objectName, uploadId, nextPartNumberMarker);
            } catch (InvalidBucketNameException | NoSuchAlgorithmException | InsufficientDataException | IOException
                     | InvalidKeyException | NoResponseException | XmlPullParserException | ErrorResponseException
                     | InternalException | InvalidResponseException e) {
              this.error = new Result<>(null, e);
            } finally {
              if (this.listPartsResult != null) {
                this.partIterator = this.listPartsResult.partList().iterator();
              } else {
                this.partIterator = new LinkedList<Part>().iterator();
              }
            }
          }

          @Override
          public boolean hasNext() {
            if (this.completed) {
              return false;
            }

            if (this.error == null && this.partIterator == null) {
              populate();
            }

            if (this.error == null && !this.partIterator.hasNext() && this.listPartsResult.isTruncated()) {
              this.nextPartNumberMarker = this.listPartsResult.nextPartNumberMarker();
              populate();
            }

            if (this.error != null) {
              return true;
            }

            if (this.partIterator.hasNext()) {
              return true;
            }

            this.completed = true;
            return false;
          }

          @Override
          public Result<Part> next() {
            if (this.completed) {
              throw new NoSuchElementException();
            }

            if (this.error == null && this.partIterator == null) {
              populate();
            }

            if (this.error == null && !this.partIterator.hasNext() && this.listPartsResult.isTruncated()) {
              this.nextPartNumberMarker = this.listPartsResult.nextPartNumberMarker();
              populate();
            }

            if (this.error != null) {
              this.completed = true;
              return this.error;
            }

            if (this.partIterator.hasNext()) {
              return new Result<>(this.partIterator.next(), null);
            }

            this.completed = true;
            throw new NoSuchElementException();
          }

          @Override
          public void remove() {
            throw new UnsupportedOperationException();
          }
        };
      }
    };
  }


  /**
   * Executes list object parts for given bucket name, object name, upload ID and part number marker and
   * returns {@link ListPartsResult}.
   */
  private ListPartsResult listObjectParts(String bucketName, String objectName, String uploadId, int partNumberMarker)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException {
    Map<String,String> queryParamMap = new HashMap<>();
    queryParamMap.put(UPLOAD_ID, uploadId);
    if (partNumberMarker > 0) {
      queryParamMap.put("part-number-marker", Integer.toString(partNumberMarker));
    }

    HttpResponse response = executeGet(bucketName, objectName, null, queryParamMap);

    ListPartsResult result = new ListPartsResult();
    result.parseXml(response.body().charStream());
    response.body().close();
    return result;
  }


  /**
   * Aborts multipart upload of given bucket name, object name and upload ID.
   */
  private void abortMultipartUpload(String bucketName, String objectName, String uploadId)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException {
    Map<String,String> queryParamMap = new HashMap<>();
    queryParamMap.put(UPLOAD_ID, uploadId);
    executeDelete(bucketName, objectName, queryParamMap);
  }


  /**
   * Removes incomplete multipart upload of given object.
   *
   * </p><b>Example:</b><br>
   * <pre>{@code minioClient.removeIncompleteUpload("my-bucketname", "my-objectname");
   * System.out.println("successfully removed all incomplete upload session of my-bucketname/my-objectname"); }</pre>
   *
   * @param bucketName Bucket name.
   * @param objectName Object name in the bucket.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *           upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *           InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *           upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidResponseException    upon a non-xml response from server
   */
  public void removeIncompleteUpload(String bucketName, String objectName)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
           InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
           InternalException, InvalidResponseException {
    for (Result<Upload> r : listIncompleteUploads(bucketName, objectName, true, false)) {
      Upload upload = r.get();
      if (objectName.equals(upload.objectName())) {
        abortMultipartUpload(bucketName, objectName, upload.uploadId());
        return;
      }
    }
  }

  /**
   * Listen to bucket notifications.
   *
   * @param bucketName Bucket name.
   * @param prefix Prefix of concerned objects events.
   * @param suffix Suffix of concerned objects events.
   * @param events List of events to watch.
   * @param eventCallback Event handler.
   *
   * @throws InvalidBucketNameException  upon invalid bucket name is given
   * @throws NoSuchAlgorithmException
   *            upon requested algorithm was not found during signature calculation
   * @throws InsufficientDataException  upon getting EOFException while reading given
   *            InputStream even before reading given length
   * @throws IOException                 upon connection error
   * @throws InvalidKeyException
   *            upon an invalid access key or secret key
   * @throws NoResponseException         upon no response from server
   * @throws XmlPullParserException      upon parsing response xml
   * @throws ErrorResponseException      upon unsuccessful execution
   * @throws InternalException           upon internal library error
   * @throws InvalidResponseException    upon a non-xml response from server
   *
   */

  public void listenBucketNotification(String bucketName, String prefix, String suffix, String[] events,
      BucketEventListener eventCallback)
    throws InvalidBucketNameException, NoSuchAlgorithmException, InsufficientDataException, IOException,
                    InvalidKeyException, NoResponseException, XmlPullParserException, ErrorResponseException,
                    InternalException, InvalidResponseException  {

    Multimap<String,String> queryParamMap = HashMultimap.create();
    queryParamMap.put("prefix", prefix);
    queryParamMap.put("suffix", suffix);
    for (String event: events) {
      queryParamMap.put("events", event);
    }

    String bodyContent = "";
    Scanner scanner = null;
    HttpResponse response = null;
    ObjectMapper mapper = new ObjectMapper();

    try {
      response = executeReq(Method.GET, getRegion(bucketName),
          bucketName, "", null, queryParamMap, null, 0);
      scanner = new Scanner(response.body().charStream());
      scanner.useDelimiter("\n");
      while (scanner.hasNext()) {
        bodyContent = scanner.next().trim();
        if (bodyContent.equals("")) {
          continue;
        }
        NotificationInfo ni = mapper.readValue(bodyContent, NotificationInfo.class);
        eventCallback.updateEvent(ni);
      }
    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw e;
    } finally {
      if (response != null) {
        response.body().close();
      }
      if (scanner != null) {
        scanner.close();
      }
    }
  }


  /**
   * Calculates multipart size of given size and returns three element array contains part size, part count
   * and last part size.
   */
  private static int[] calculateMultipartSize(long size)
    throws InvalidArgumentException {
    if (size > MAX_OBJECT_SIZE) {
      throw new InvalidArgumentException("size " + size + " is greater than allowed size 5TiB");
    }

    double partSize = Math.ceil((double) size / MAX_MULTIPART_COUNT);
    partSize = Math.ceil(partSize / MIN_MULTIPART_SIZE) * MIN_MULTIPART_SIZE;

    double partCount = Math.ceil(size / partSize);

    double lastPartSize = partSize - (partSize * partCount - size);
    if (lastPartSize == 0.0) {
      lastPartSize = partSize;
    }

    return new int[] { (int) partSize, (int) partCount, (int) lastPartSize };
  }

  /**
   * Return available size of given input stream up to given expected read size.  If less data is available than
   * expected read size, it returns how much data available to read.
   */
  private int getAvailableSize(Object inputStream, int expectedReadSize) throws IOException, InternalException {
    RandomAccessFile file = null;
    BufferedInputStream stream = null;
    if (inputStream instanceof RandomAccessFile) {
      file = (RandomAccessFile) inputStream;
    } else if (inputStream instanceof BufferedInputStream) {
      stream = (BufferedInputStream) inputStream;
    } else {
      throw new InternalException("Unknown input stream. This should not happen.  "
                                  + "Please report to https://github.com/minio/minio-java/issues/");
    }

    // hold current position of file/stream to reset back to this position.
    long pos = 0;
    if (file != null) {
      pos = file.getFilePointer();
    } else {
      stream.mark(expectedReadSize);
    }

    // 16KiB buffer for optimization
    byte[] buf = new byte[16384];
    int bytesToRead = buf.length;
    int bytesRead = 0;
    int totalBytesRead = 0;
    while (totalBytesRead < expectedReadSize) {
      if ((expectedReadSize - totalBytesRead) < bytesToRead) {
        bytesToRead = expectedReadSize - totalBytesRead;
      }

      if (file != null) {
        bytesRead = file.read(buf, 0, bytesToRead);
      } else {
        bytesRead = stream.read(buf, 0, bytesToRead);
      }

      if (bytesRead < 0) {
        // reached EOF
        break;
      }

      totalBytesRead += bytesRead;
    }

    // reset back to saved position.
    if (file != null) {
      file.seek(pos);
    } else {
      stream.reset();
    }

    return totalBytesRead;
  }


  /**
   * Enables HTTP call tracing and written to traceStream.
   *
   * @param traceStream {@link OutputStream} for writing HTTP call tracing.
   *
   * @see #traceOff
   */
  public void traceOn(OutputStream traceStream) {
    if (traceStream == null) {
      throw new NullPointerException();
    } else {
      this.traceStream = new PrintWriter(new OutputStreamWriter(traceStream, StandardCharsets.UTF_8), true);
    }
  }


  /**
   * Disables HTTP call tracing previously enabled.
   *
   * @see #traceOn
   * @throws IOException                 upon connection error
   */
  public void traceOff() throws IOException {
    this.traceStream = null;
  }
}
