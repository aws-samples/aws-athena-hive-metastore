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
package com.amazonaws.athena.hms.serde;

import com.amazonaws.athena.hms.ApiHelper;
import com.amazonaws.athena.hms.ApiResponse;
import com.amazonaws.athena.hms.MetadataResponse;
import com.amazonaws.athena.hms.io.S3Helper;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

import static com.amazonaws.athena.hms.MetadataRequest.API_NAME;
import static com.amazonaws.athena.hms.MetadataResponse.API_RESPONSE;
import static com.amazonaws.athena.hms.MetadataResponse.IS_SPILLED;
import static com.amazonaws.athena.hms.MetadataResponse.SPILL_PATH;

public class MetadataResponseDeserializer extends StdDeserializer<MetadataResponse>
{
  private static final String ERROR_MESSAGE = "errorMessage";
  private static final String STACK_TRACE = "stackTrace";
  private final ApiHelper apiHelper;
  private final S3Helper s3Helper;

  protected MetadataResponseDeserializer(ApiHelper apiHelper, S3Helper s3Helper)
  {
    super(MetadataResponse.class);
    this.apiHelper = apiHelper;
    this.s3Helper = s3Helper;
  }

  @Override
  public MetadataResponse deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException, JsonProcessingException
  {
    if (jsonParser.getCurrentToken() != JsonToken.START_OBJECT) {
      throw new IOException("Expected start object.");
    }

    String errorMessage = null;
    String stackTrace = null;
    String apiName = null;
    boolean isSpilled = false;
    String spillPath = null;
    ApiResponse apiResponse = null;
    while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
      if (jsonParser.getCurrentName() == null) {
        // for error message currentName() could return null at the end
        break;
      }
      switch (jsonParser.getCurrentName()) {
        case ERROR_MESSAGE:
          // move to field value
          jsonParser.nextToken();
          errorMessage = jsonParser.getValueAsString();
          break;
        case STACK_TRACE:
          jsonParser.nextToken();
          stackTrace = jsonParser.getValueAsString();
          break;
        case API_NAME:
          // move to field value
          jsonParser.nextToken();
          apiName = jsonParser.getValueAsString();
          break;
        case IS_SPILLED:
          // move to field value
          jsonParser.nextToken();
          isSpilled = jsonParser.getValueAsBoolean();
          break;
        case SPILL_PATH:
          // move to field value
          jsonParser.nextToken();
          spillPath = jsonParser.getValueAsString();
          break;
        case API_RESPONSE:
          // move to field value
          jsonParser.nextToken();
          // get the request class type
          Class responseClass = apiHelper.getResponseClass(apiName);
          if (responseClass == null) {
            throw new IOException("Cannot find response class for " + apiName);
          }
          if (isSpilled) {
            // Need to read from s3 and convert the String content into the response object
            apiResponse = (ApiResponse) s3Helper.getResponseFromS3As(responseClass, spillPath);
          }
          else {
            // deserialize value based on the response class type
            apiResponse = (ApiResponse) jsonParser.readValueAs(responseClass);
          }
          break;
      }
    }

    if (errorMessage != null) {
      throw new IOException(errorMessage, new Throwable(stackTrace));
    }

    return new MetadataResponse(apiName, isSpilled, spillPath, apiResponse);
  }
}
