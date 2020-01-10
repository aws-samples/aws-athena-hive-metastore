/*-
 * #%L
 * hms-lambda-handler
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
package com.amazonaws.athena.hms;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.athena.hms.io.S3Helper;
import com.amazonaws.athena.hms.serde.ObjectMapperFactory;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestStreamHandler;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

// a delegator to dispatch requests to different handlers based on the ApiName in the request object
// a single Lambda function that supports all Hive Metastore calls should extend this class
public class MetadataHandler implements RequestStreamHandler
{
  // the s3 path to spill response object to if the response size exceeds a limit
  private final String responseSpillLocation;

  // the threshold that we use to decide whether to spill to s3
  // Please be aware that it is difficult to get the exact size of the response object due
  // to the way how a Java object is represented in memory, we should set this limit much less than the
  // Lambda hard limit 6MB, for example, we could set it as 4MB
  private final long responseSpillThreshold;

  // the mapping between API name and a HandlerContext, which includes the handler and request/response class types
  private final Map<String, HandlerContext> handlers;

  // hive metastore configuration
  private final HiveMetaStoreConf conf;
  // hive metastore client
  private final HiveMetaStoreClient client;

  private final ApiHelper apiHelper;
  private final AmazonS3 s3Client;
  private final S3Helper s3Helper;
  private final ObjectMapper objectMapper;
  private final HandlerProvider handlerProvider;

  public MetadataHandler(HiveMetaStoreClientFactory factory)
  {
    this.conf = factory.getConf();
    this.client = factory.getHiveMetaStoreClient();
    this.handlerProvider = factory.getHandlerProvider();
    this.responseSpillLocation = conf.getResponseSpillLocation();
    this.responseSpillThreshold = conf.getResponseSpillThreshold();
    this.apiHelper = new ApiHelper();
    this.s3Client = buildS3Client();
    this.s3Helper = new S3Helper(s3Client);
    this.objectMapper = ObjectMapperFactory.create(apiHelper, s3Helper);
    this.handlers = handlerProvider.provide(conf, client);
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

  private String spillToS3(Context context, String responseAsString)
  {
    // get the actual s3 path
    String s3Path = responseSpillLocation + "/" + context.getFunctionName() + "/" + context.getAwsRequestId();
    // log the s3 path in case invalid s3 path causes exceptions
    context.getLogger().log("Saving response to s3: " + s3Path);
    s3Helper.saveResponseToS3(s3Path, responseAsString);
    return s3Path;
  }

  @Override
  public void handleRequest(InputStream inputStream, OutputStream outputStream, Context context) throws IOException
  {
    try (MetadataRequest metadataRequest = objectMapper.readValue(inputStream, MetadataRequest.class)) {
      // cloud watch logs, we need them for monitoring and debugging
      context.getLogger().log("HMS configuration: " + conf.toString());
      context.getLogger().log("RequestContext: " + metadataRequest.getContext());
      String apiName = metadataRequest.getApiName();
      context.getLogger().log("API: " + apiName);
      HandlerContext handlerContext = handlers.get(apiName);
      if (handlerContext == null) {
        throw new RuntimeException("Cannot find handler for API " + apiName);
      }
      ApiRequest apiRequest = metadataRequest.getApiRequest();
      ApiResponse apiResponse = (ApiResponse) handlerContext.getHandler().handleRequest(apiRequest, context);
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
    }
    catch (Exception e) {
      throw new IOException(e);
    }
  }
}
