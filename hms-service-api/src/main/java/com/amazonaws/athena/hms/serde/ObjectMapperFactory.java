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
import com.amazonaws.athena.hms.MetadataResponse;
import com.amazonaws.athena.hms.io.S3Helper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;

public class ObjectMapperFactory
{
  private ObjectMapperFactory()
  {
  }

  public static ObjectMapper create(ApiHelper helper, S3Helper s3Helper)
  {
    ObjectMapper objectMapper = new ObjectMapper();
    SimpleModule module = new SimpleModule();
    module.addSerializer(MetadataRequest.class, new MetadataRequestSerializer(helper));
    module.addDeserializer(MetadataRequest.class, new MetadataRequestDeserializer(helper));
    module.addSerializer(MetadataResponse.class, new MetadataResponseSerializer(helper));
    module.addDeserializer(MetadataResponse.class, new MetadataResponseDeserializer(helper, s3Helper));
    objectMapper.registerModule(module)
        .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
        .enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
    return objectMapper;
  }
}
