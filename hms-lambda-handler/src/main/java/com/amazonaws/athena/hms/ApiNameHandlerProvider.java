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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class ApiNameHandlerProvider implements HandlerProvider
{
  private final ApiHelper apiHelper;

  public ApiNameHandlerProvider(ApiHelper apiHelper)
  {
    this.apiHelper = apiHelper;
  }

  @Override
  public Map<String, HandlerContext> provide(HiveMetaStoreConf conf, HiveMetaStoreClient metaStoreClient)
  {
    Map<String, HandlerContext> map = new HashMap<>();
    Set<String> apiNames = apiHelper.getApiNames();
    for (String name : apiNames) {
      // the handler name convention is the camel case of the API name + "Handler"
      String handlerName = conf.getHandlerNamePrefix() + name.substring(0, 1).toUpperCase() + name.substring(1) + "Handler";
      try {
        Class<? extends BaseHMSHandler> clazz = Class.forName(handlerName).asSubclass(BaseHMSHandler.class);
        Constructor<? extends BaseHMSHandler> constructor =
            clazz.getConstructor(HiveMetaStoreConf.class, HiveMetaStoreClient.class);
        BaseHMSHandler handler = constructor.newInstance(conf, metaStoreClient);
        HandlerContext context = new HandlerContext(handler, apiHelper.getRequestClass(name), apiHelper.getResponseClass(name));
        map.put(name, context);
      }
      catch (ClassNotFoundException | NoSuchMethodException | InstantiationException | IllegalAccessException | InvocationTargetException e) {
        throw new RuntimeException(e);
      }
    }

    return map;
  }
}
