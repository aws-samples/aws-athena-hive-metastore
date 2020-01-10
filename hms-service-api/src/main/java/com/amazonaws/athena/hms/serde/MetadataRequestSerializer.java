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
import com.amazonaws.athena.hms.MetadataRequest;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

import static com.amazonaws.athena.hms.MetadataRequest.API_NAME;
import static com.amazonaws.athena.hms.MetadataRequest.API_REQUEST;
import static com.amazonaws.athena.hms.MetadataRequest.REQUEST_CONTEXT;

public class MetadataRequestSerializer extends StdSerializer<MetadataRequest>
{
  private final ApiHelper apiHelper;
  protected MetadataRequestSerializer(ApiHelper apiHelper)
  {
    super(MetadataRequest.class);
    this.apiHelper = apiHelper;
  }

  @Override
  public void serialize(MetadataRequest metadataRequest, JsonGenerator jsonGenerator,
                        SerializerProvider serializerProvider) throws IOException
  {
    String apiName = metadataRequest.getApiName();
    if (apiName == null) {
      throw new IOException("ApiName cannot be null");
    }
    jsonGenerator.writeStartObject();
    jsonGenerator.writeObjectField(REQUEST_CONTEXT, metadataRequest.getContext());
    jsonGenerator.writeStringField(API_NAME, apiName);
    jsonGenerator.writeObjectField(API_REQUEST, metadataRequest.getApiRequest());
    jsonGenerator.writeEndObject();
  }
}
