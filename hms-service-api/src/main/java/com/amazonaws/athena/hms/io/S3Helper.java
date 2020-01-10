/*-
 * #%L
 * hms-service-api
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.hms.io;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class S3Helper
{
  private final AmazonS3 s3Client;
  private final ObjectMapper objectMapper;

  public S3Helper(AmazonS3 s3Client)
  {
    this.s3Client = s3Client;
    this.objectMapper = new ObjectMapper();
  }

  public void saveResponseToS3(String s3Path, String responseAsString)
  {
    AmazonS3URI s3URI = new AmazonS3URI(s3Path);
    String bucket = s3URI.getBucket();
    String key = s3URI.getKey();
    s3Client.putObject(bucket, key, responseAsString);
  }

  public String getResponseFromS3(String s3Path) throws IOException
  {
    if (s3Path == null) {
      throw new IOException("S3 path is null");
    }
    AmazonS3URI s3URI = new AmazonS3URI(s3Path);
    String bucket = s3URI.getBucket();
    String key = s3URI.getKey();
    BufferedReader br = new BufferedReader(new InputStreamReader(
        s3Client.getObject(new GetObjectRequest(bucket, key)).getObjectContent()));

    String line;
    StringBuilder sb = new StringBuilder();
    // read the files until it reaches the end of the file
    while ((line = br.readLine()) != null) {
      sb.append(line);
    }

    return sb.toString();
  }

  public <T> T getResponseFromS3As(Class<T> clazz, String s3Path) throws IOException
  {
    String responseAsString = getResponseFromS3(s3Path);
    return objectMapper.readValue(responseAsString, clazz);
  }
}
