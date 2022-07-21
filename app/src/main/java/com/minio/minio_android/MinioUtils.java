package com.minio.minio_android;


import android.util.Log;

import com.minio.minio_android.errors.ErrorResponseException;
import com.minio.minio_android.errors.InsufficientDataException;
import com.minio.minio_android.errors.InternalException;
import com.minio.minio_android.errors.InvalidArgumentException;
import com.minio.minio_android.errors.InvalidBucketNameException;
import com.minio.minio_android.errors.InvalidEndpointException;
import com.minio.minio_android.errors.InvalidPortException;
import com.minio.minio_android.errors.InvalidResponseException;
import com.minio.minio_android.errors.NoResponseException;

import org.xmlpull.v1.XmlPullParserException;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

public class MinioUtils {
    private static final String TAG = MinioUtils.class.getSimpleName();

    private String BUCKET_NAME = "test";
    private String END_POINT = "http://192.168.10.40:9000";
    private String ACCOUNT = "huang";
    private String SECRET_KEY = "asdfghj999";

    MinioUtils() {}

    public MinioUtils resetEndPoint(String endPoint){
        END_POINT = endPoint;
        return this;
    }

    public MinioUtils resetAccount(String account){
        ACCOUNT = account;
        return this;
    }

    public MinioUtils resetSecretKey(String secretKey){
        SECRET_KEY = secretKey;
        return this;
    }

    public MinioUtils resetBucketName(String bucketName){
        BUCKET_NAME = bucketName;
        return this;
    }

    public void upload(String filepath, String objectName) {
        try {
            // Create a minioClient
            MinioClient minioClient = new MinioClient(END_POINT,
                                                      ACCOUNT,
                                                      SECRET_KEY);

            minioClient.putObject(BUCKET_NAME, objectName, filepath, null, null, null, null);
            Log.d(TAG,
                    filepath + " is successfully uploaded as "
                            + "object " + objectName + " to bucket test.");

        } catch (RuntimeException | InvalidEndpointException | InvalidPortException e) {
            Log.e(TAG, String.valueOf(e.getCause()));
            e.printStackTrace();
        } catch (InvalidArgumentException
                | NoResponseException
                | InvalidBucketNameException
                | InsufficientDataException
                | XmlPullParserException
                | ErrorResponseException
                | NoSuchAlgorithmException
                | IOException
                | InvalidKeyException
                | InvalidResponseException
                | InternalException e) {
            e.printStackTrace();
        }
    }

    public void upload(String filepath, String objectName, uploadedCallback cb){
        upload(filepath, objectName);
        cb.onUploaded(filepath);
    }

    public void download(String objectName, downloadCallback cb) {
        try {
            MinioClient minioClient = new MinioClient(END_POINT,
                                                        ACCOUNT,
                                                    SECRET_KEY);

            InputStream is = minioClient.getObject(BUCKET_NAME, objectName);
            cb.onDownload(is);
        } catch (InvalidEndpointException | InvalidPortException e) {
            Log.e(TAG, String.valueOf(e.getCause()));
            e.printStackTrace();
        } catch (InvalidArgumentException
                | InvalidBucketNameException
                | InsufficientDataException
                | XmlPullParserException
                | ErrorResponseException
                | NoSuchAlgorithmException
                | IOException
                | NoResponseException
                | InvalidResponseException
                | InternalException
                | InvalidKeyException e) {
            e.printStackTrace();
        }
    }

    public void download(String objectName, String to){
        download(objectName, (is)-> {
            //创建文件的file对象
            File file = new File(to);

            //将数据流写进to文件
            try {
                FileOutputStream fos = new FileOutputStream(file, true);

                byte[] buffer = new byte[2048];
                int count = 0;
                while ((count = is.read(buffer)) > 0) {
                    fos.write(buffer, 0, count);
                }

                fos.close();
                Log.d(TAG,
                        objectName + " is successfully downloaded to "
                                 + to + " .");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } finally {
                is.close();
            }
        });
    }

    public interface uploadedCallback {
        void onUploaded(String filepath);
    }
    public interface downloadCallback {
        void onDownload(InputStream inputStream) throws IOException;
    }
}