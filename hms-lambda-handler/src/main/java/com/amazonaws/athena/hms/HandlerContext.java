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

import com.amazonaws.athena.hms.handler.BaseHMSHandler;

public class HandlerContext
{
  // the request handler
  private final BaseHMSHandler handler;
  // the request class, we need this for deserialization
  private final Class<? extends ApiRequest> requestClass;
  // the response class, we need this for serialization
  private final Class<? extends ApiResponse> responseClass;

  public HandlerContext(BaseHMSHandler handler, Class<? extends ApiRequest> requestClass, Class<? extends ApiResponse> responseClass)
  {
    this.handler = handler;
    this.requestClass = requestClass;
    this.responseClass = responseClass;
  }

  public Class<? extends ApiRequest> getRequestClass()
  {
    return requestClass;
  }

  public Class<? extends ApiResponse> getResponseClass()
  {
    return responseClass;
  }

  public BaseHMSHandler getHandler()
  {
    return handler;
  }
}
