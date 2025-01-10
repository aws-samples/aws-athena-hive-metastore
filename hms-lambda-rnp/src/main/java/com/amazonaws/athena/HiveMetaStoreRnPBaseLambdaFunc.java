/*-
 * #%L
 * hms-lambda-rnp
 * %%
 * Copyright (C) 2019 - 2020 Amazon Web Services
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
package com.amazonaws.athena;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.athena.hms.ApiHelper;
import com.amazonaws.athena.hms.ApiRequest;
import com.amazonaws.athena.hms.ApiResponse;
import com.amazonaws.athena.hms.HandlerContext;
import com.amazonaws.athena.hms.HandlerProvider;
import com.amazonaws.athena.hms.HiveMetaStoreClient;
import com.amazonaws.athena.hms.HiveMetaStoreClientFactory;
import com.amazonaws.athena.hms.MetadataRequest;
import com.amazonaws.athena.hms.MetadataResponse;
import com.amazonaws.athena.hms.io.S3Helper;
import com.amazonaws.athena.hms.serde.ObjectMapperFactory;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Map;

// clone MetadataHandler logic here to avoid changing it since this func
// should only be used for our internal mock tests
public abstract class HiveMetaStoreRnPBaseLambdaFunc implements RequestStreamHandler
{
  // the s3 path to spill response object to if the response size exceeds a limit
  protected final String responseSpillLocation;

  // the threshold that we use to decide whether to spill to s3
  // Please be aware that it is difficult to get the exact size of the response object due
  // to the way how a Java object is represented in memory, we should set this limit much less than the
  // Lambda hard limit 6MB, for example, we could set it as 4MB
  protected final long responseSpillThreshold;

  // record response in a s3 location when function works in the RECORD mode and it replays the
  // response from this location when it is in the REPLAY mode
  protected final String responseRecordLocation;

  // the mapping between API name and a HandlerContext, which includes the handler and request/response class types
  protected final Map<String, HandlerContext> handlers;

  // hive metastore configuration
  protected final MockHiveMetaStoreConf conf;
  // hive metastore client
  protected final HiveMetaStoreClient client;

  protected final ApiHelper apiHelper;
  protected final AmazonS3 s3Client;
  protected final S3Helper s3Helper;
  protected final ObjectMapper objectMapper;
  protected final HandlerProvider handlerProvider;
  protected final Gson gson;
  protected final RNPMode mode;

  public HiveMetaStoreRnPBaseLambdaFunc(HiveMetaStoreClientFactory factory, RNPMode mode)
  {
    this.conf = MockHiveMetaStoreConf.loadAndOverrideExtraEnvironmentVariables();
    this.mode = mode;
    this.client = factory.getHiveMetaStoreClient();
    this.handlerProvider = factory.getHandlerProvider();
    this.responseSpillLocation = conf.getResponseSpillLocation();
    this.responseSpillThreshold = conf.getResponseSpillThreshold();
    this.responseRecordLocation = conf.getResponseRecordLocation();
    this.apiHelper = new ApiHelper();
    this.s3Client = buildS3Client();
    this.s3Helper = new S3Helper(s3Client);
    this.objectMapper = ObjectMapperFactory.create(apiHelper, s3Helper);
    this.handlers = handlerProvider.provide(conf, client);
    this.gson = new Gson();
  }

  private AmazonS3 buildS3Client()
  {
    ClientConfiguration configuration =
        new ClientConfiguration()
            .withConnectionTimeout(60000)
            .withClientExecutionTimeout(900000)
            .withSocketTimeout(60000)
            .withMaxErrorRetry(10);

    return AmazonS3ClientBuilder.standard()
        // For S3 VPCE endpoint, we still need to use the regular s3 endpoint since it is a gateway
        .withClientConfiguration(configuration)
        .build();
  }

  protected String spillToS3(Context context, String responseAsString)
  {
    // get the actual s3 path
    String s3Path = responseSpillLocation + "/" + context.getFunctionName() + "/" + context.getAwsRequestId();
    // log the s3 path in case invalid s3 path causes exceptions
    context.getLogger().log("Saving response to s3: " + s3Path);
    s3Helper.saveResponseToS3(s3Path, responseAsString);
    return s3Path;
  }

  protected void recordToS3(Context context, MetadataRequest request, String responseAsString)
      throws NoSuchAlgorithmException
  {
    String s3Path = responseRecordLocation  + "/" + context.getFunctionName() + "/" + request.getApiName()
        + "/" + getRequestMd5(request.getApiRequest());
    context.getLogger().log("Recording response to s3: " + s3Path);
    s3Helper.saveResponseToS3(s3Path, responseAsString);
  }

  protected String getRequestMd5(ApiRequest apiRequest) throws NoSuchAlgorithmException
  {
    MessageDigest m = MessageDigest.getInstance("MD5");
    m.reset();
    m.update(gson.toJson(apiRequest).getBytes());
    byte[] digest = m.digest();
    BigInteger bigInt = new BigInteger(1, digest);
    String hashtext = bigInt.toString(16);
    StringBuffer sb = new StringBuffer();
    for (int i = hashtext.length() + 1; i <= 32; i++) {
      sb.append("0");
    }
    sb.append(hashtext);
    return sb.toString();
  }

  ApiResponse getResponseDirectly(Context context, HandlerContext handlerContext, String apiName, ApiRequest apiRequest)
  {
    ApiResponse apiResponse = (ApiResponse) handlerContext.getHandler().handleRequest(apiRequest, context);
    return apiResponse;
  }

  ApiResponse getResponseFromS3(Context context, HandlerContext handlerContext, String apiName, ApiRequest apiRequest)
  {
    try {
      String s3Path = conf.getResponseRecordLocation()  + "/" + context.getFunctionName() + "/" + apiName
          + "/" + getRequestMd5(apiRequest);
      context.getLogger().log("Replaying response from s3: " + s3Path);
      String response = s3Helper.getResponseFromS3(s3Path);
      ApiResponse apiResponse = (ApiResponse) objectMapper.readValue(response, handlerContext.getResponseClass());
      return apiResponse;
    }
    catch (NoSuchAlgorithmException e) {
      throw new RuntimeException("MD5 isn't supported", e);
    }
    catch (IOException e) {
      throw new RuntimeException("Failed to fetch response from s3", e);
    }
  }

  @Override
  public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException
  {
    try (MetadataRequest metadataRequest = objectMapper.readValue(inputStream, MetadataRequest.class)) {
      // cloud watch logs, we need them for monitoring and debugging
      context.getLogger().log("HMS configuration: " + conf.toString());
      context.getLogger().log("Lambda Func in mode: " + mode);
      context.getLogger().log("RequestContext: " + metadataRequest.getContext());
      String apiName = metadataRequest.getApiName();
      context.getLogger().log("API: " + apiName);
      HandlerContext handlerContext = handlers.get(apiName);
      if (handlerContext == null) {
        throw new RuntimeException("Cannot find handler for API " + apiName);
      }
      ApiRequest apiRequest = metadataRequest.getApiRequest();
      ApiResponse apiResponse;
      if (mode == RNPMode.REPLAY) {
        apiResponse = getResponseFromS3(context, handlerContext, apiName, apiRequest);
      }
      else {
        apiResponse = getResponseDirectly(context, handlerContext, apiName, apiRequest);
      }
      // serialize ApiResponse to String to get its size. Please be aware this isn't accurate
      String responseAsString = objectMapper.writerFor(handlerContext.getResponseClass()).writeValueAsString(apiResponse);
      // get the response String size
      long responseSize = responseAsString.getBytes().length;
      context.getLogger().log("Response size: " + responseSize);
      MetadataResponse response;
      if (responseSize >= responseSpillThreshold) {
        String spillPath = spillToS3(context, responseAsString);
        context.getLogger().log("Response size " + responseAsString.length() + " exceeded threshold "
            + responseSpillThreshold + ", is saved to s3: " + spillPath);
        response = new MetadataResponse(apiName, true, spillPath, null);
      }
      else {
        response = new MetadataResponse(apiName, false, null, apiResponse);
      }
      objectMapper.writeValue(outputStream, response);
      if (mode == RNPMode.RECORD) {
        recordToS3(context, metadataRequest, responseAsString);
      }
    }
    catch (Exception e) {
      throw new IOException(e);
    }
  }
}
