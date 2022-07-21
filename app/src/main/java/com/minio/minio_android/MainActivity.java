package com.minio.minio_android;

import androidx.annotation.RequiresApi;
import androidx.appcompat.app.AppCompatActivity;
import androidx.core.content.ContextCompat;

import android.Manifest;
import android.app.Activity;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.os.Environment;
import android.text.method.ScrollingMovementMethod;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.TextView;
import android.widget.Toast;
import android.widget.ToggleButton;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class MainActivity extends AppCompatActivity {

    Button button1;
    MinioUtils client = new MinioUtils();
    private static final int STORAGE_PERM = 1;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        ToggleButton actionSelector = findViewById(R.id.action);
        EditText endpointText = findViewById(R.id.endpoint);
        EditText accountText = findViewById(R.id.account);
        EditText passwordText = findViewById(R.id.password);
        EditText pathText = findViewById(R.id.path);
        EditText bucketNameText = findViewById(R.id.bucket);
        TextView infoText = findViewById(R.id.info);

        infoText.setMovementMethod(ScrollingMovementMethod.getInstance());

        requestForStoragePerm();

        button1 = findViewById(R.id.button);
        button1.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                if ("Upload".contentEquals(actionSelector.getText())) {
                    new Thread(() -> {
                        // Upload
                        // Todo: Batch upload
                        infoText.setText("Upload starting...\n");
                        client.resetAccount(accountText.getText().toString())
                                .resetSecretKey(passwordText.getText().toString())
                                .resetEndPoint(endpointText.getText().toString())
                                .resetBucketName(bucketNameText.getText().toString())
                                .upload(Environment.getExternalStorageDirectory().getPath() + pathText.getText(),
                                        pathText.getText().toString());
                        infoText.append("Upload Success!\n");
                    }).start();
                } else if ("Download".contentEquals(actionSelector.getText())) {
                    new Thread(() -> {
                        // Download
                        infoText.setText("Download starting...\n");
                        client.resetAccount(accountText.getText().toString())
                                .resetSecretKey(passwordText.getText().toString())
                                .resetEndPoint(endpointText.getText().toString())
                                .resetBucketName(bucketNameText.getText().toString())
                                .download(pathText.getText().toString(),
                                        Environment.getExternalStorageDirectory().getPath() + pathText.getText());

                        client.resetAccount(accountText.getText().toString())
                                .resetSecretKey(passwordText.getText().toString())
                                .resetEndPoint(endpointText.getText().toString())
                                .resetBucketName(bucketNameText.getText().toString())
                                .download(pathText.getText().toString(),
                                        (is)-> {
                                            byte[] buffer = new byte[2048];
                                            while (is.read(buffer) > 0)
                                                infoText.append(new String(buffer) + "\n");
                                        });
                        infoText.append("Download Success!\n");
//                        client.resetBucketName("test")
//                                .upload(Environment.getExternalStorageDirectory().getPath()+"/Download/1001.doc",
//                                        "1001.doc");
                    }).start();
                }
//                new Thread(() -> {
//                    // Upload
//                    MinioUtils. upload(Environment.getExternalStorageDirectory().getPath()+"/Download/1001.doc",
//                                "1001.doc");
//                    // Download
//                    MinioUtils.download("1001.doc",
//                            Environment.getExternalStorageDirectory().getPath()+"/Download/1001.doc");
//
//                    client.resetBucketName("test")
//                            .upload(Environment.getExternalStorageDirectory().getPath()+"/Download/1001.doc",
//                                "1001.doc");
//                }).start();

            }
        });
    }

    @RequiresApi(api = Build.VERSION_CODES.M)
    private void requestForStoragePerm(){
        if (ContextCompat.checkSelfPermission(
                this, Manifest.permission.WRITE_EXTERNAL_STORAGE) ==
                PackageManager.PERMISSION_GRANTED) {
        } else {
            // You can directly ask for the permission.
            requestPermissions(new String[] { Manifest.permission.WRITE_EXTERNAL_STORAGE},
                    STORAGE_PERM);
        }
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, String[] permissions,
                                           int[] grantResults) {
        super.onRequestPermissionsResult(requestCode, permissions, grantResults);
        switch (requestCode) {
            case STORAGE_PERM:
                // If request is cancelled, the result arrays are empty.
                if (grantResults.length > 0 &&
                        grantResults[0] == PackageManager.PERMISSION_GRANTED) {
                    // Permission is granted. Continue the action or workflow
                    // in your app.
                } else {
                    // Explain to the user that the feature is unavailable because
                    // the features requires a permission that the user has denied.
                    // At the same time, respect the user's decision. Don't link to
                    // system settings in an effort to convince the user to change
                    // their decision.
                    Toast.makeText(this, "req failed!", Toast.LENGTH_SHORT).show();
                }
                return;
        }
        // Other 'case' lines to check for other
        // permissions this app might request.
    }

}
