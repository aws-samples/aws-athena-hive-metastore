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
package com.amazonaws.athena.hms;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public final class ApiHelper<REQUEST extends ApiRequest, RESPONSE extends ApiResponse>
{
  private final Map<Class<REQUEST>, ApiContext<REQUEST, RESPONSE>> apiContextMap;
  private final Map<String, ApiContext<REQUEST, RESPONSE>> apiNameMap;

  public ApiHelper()
  {
    apiContextMap = new HashMap();
    apiNameMap = new HashMap<>();
    populateApiContexts();
  }

  public String getApiName(Class<REQUEST> requestClass, Class<RESPONSE> responseClass)
  {
    ApiContext<REQUEST, RESPONSE> apiContext = apiContextMap.get(requestClass);
    if (apiContext != null && apiContext.responseClass.isAssignableFrom(responseClass)) {
     return apiContext.getApiName();
    }

    return null;
  }

  public Class<REQUEST> getRequestClass(String apiName)
  {
    ApiContext apiContext = apiNameMap.get(apiName);
    if (apiContext != null) {
      return apiContext.getRequestClass();
    }

    return null;
  }

  public Class<RESPONSE> getResponseClass(String apiName)
  {
    ApiContext apiContext = apiNameMap.get(apiName);
    if (apiContext != null) {
      return apiContext.getResponseClass();
    }

    return null;
  }

  private void populateApiContexts()
  {
    Method[] methods = HiveMetaStoreService.class.getDeclaredMethods();
    for (Method method : methods) {
      Class[] requestParameters = method.getParameterTypes();
      // HiveMetaStoreService APIs always follow request/response format
      if (requestParameters.length == 1) {
        Class requestClass = requestParameters[0];
        ApiContext<REQUEST, RESPONSE> apiContext =
            new ApiContext(method.getName(), requestParameters[0], method.getReturnType());
        apiContextMap.put(requestClass, apiContext);
        apiNameMap.put(apiContext.getApiName(), apiContext);
      }
    }
  }

  public Set<String> getApiNames()
  {
    return apiNameMap.keySet();
  }

  private static class ApiContext<REQUEST extends ApiRequest, RESPONSE extends ApiResponse>
  {
    private final String apiName;
    private final Class<REQUEST> requestClass;
    private final Class<RESPONSE> responseClass;

    ApiContext(String apiName, Class<REQUEST> requestClass, Class<RESPONSE> responseClass)
    {
      this.apiName = apiName;
      this.requestClass = requestClass;
      this.responseClass = responseClass;
    }

    String getApiName()
    {
      return apiName;
    }

    Class<REQUEST> getRequestClass()
    {
      return requestClass;
    }

    Class<RESPONSE> getResponseClass()
    {
      return responseClass;
    }
  }
}
