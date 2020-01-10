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
import com.amazonaws.athena.hms.MetadataResponse;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

import static com.amazonaws.athena.hms.MetadataResponse.API_NAME;
import static com.amazonaws.athena.hms.MetadataResponse.API_RESPONSE;
import static com.amazonaws.athena.hms.MetadataResponse.IS_SPILLED;
import static com.amazonaws.athena.hms.MetadataResponse.SPILL_PATH;

public class MetadataResponseSerializer extends StdSerializer<MetadataResponse>
{
  private final ApiHelper apiHelper;

  protected MetadataResponseSerializer(ApiHelper apiHelper)
  {
    super(MetadataResponse.class);
    this.apiHelper = apiHelper;
  }

  @Override
  public void serialize(MetadataResponse metadataResponse, JsonGenerator jsonGenerator, SerializerProvider serializerProvider) throws IOException
  {
    jsonGenerator.writeStartObject();
    jsonGenerator.writeStringField(API_NAME, metadataResponse.getApiName());
    jsonGenerator.writeBooleanField(IS_SPILLED, metadataResponse.isSpilled());
    jsonGenerator.writeStringField(SPILL_PATH, metadataResponse.getSpillPath());
    jsonGenerator.writeObjectField(API_RESPONSE, metadataResponse.getApiResponse());
    jsonGenerator.writeEndObject();
  }
}
