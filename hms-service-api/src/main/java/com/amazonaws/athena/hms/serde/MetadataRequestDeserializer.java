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
import com.amazonaws.athena.hms.ApiRequest;
import com.amazonaws.athena.hms.MetadataRequest;
import com.amazonaws.athena.hms.RequestContext;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

import java.io.IOException;

import static com.amazonaws.athena.hms.MetadataRequest.API_NAME;
import static com.amazonaws.athena.hms.MetadataRequest.API_REQUEST;
import static com.amazonaws.athena.hms.MetadataRequest.REQUEST_CONTEXT;

public class MetadataRequestDeserializer extends StdDeserializer<MetadataRequest>
{
  private final ApiHelper apiHelper;

  protected MetadataRequestDeserializer(ApiHelper apiHelper)
  {
    super(MetadataRequest.class);
    this.apiHelper = apiHelper;
  }

  @Override
  public MetadataRequest deserialize(JsonParser jsonParser, DeserializationContext deserializationContext)
      throws IOException, JsonProcessingException
  {
    if (jsonParser.getCurrentToken() != JsonToken.START_OBJECT) {
      throw new IOException("Expected start object.");
    }

    RequestContext context = null;
    String apiName = null;
    ApiRequest request = null;
    while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
      switch (jsonParser.getCurrentName()) {
        case REQUEST_CONTEXT:
          // move to field value
          jsonParser.nextToken();
          context = jsonParser.readValueAs(RequestContext.class);
          break;
        case API_NAME:
          // move to field value
          jsonParser.nextToken();
          apiName = jsonParser.getValueAsString();
          break;
        case API_REQUEST:
          // move to field value
          jsonParser.nextToken();
          // get the request class type
          Class requestClass = apiHelper.getRequestClass(apiName);
          if (requestClass == null) {
            throw new IOException("Cannot find request class for " + apiName);
          }
          // deserialize value based on the request class type
          request = (ApiRequest) jsonParser.readValueAs(requestClass);
          break;
      }
    }

    return new MetadataRequest(context, apiName, request);
  }
}
