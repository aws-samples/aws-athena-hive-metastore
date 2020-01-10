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
package com.amazonaws.athena.hms.handler;

import com.amazonaws.athena.hms.GetDatabaseNamesRequest;
import com.amazonaws.athena.hms.GetDatabaseNamesResponse;
import com.amazonaws.athena.hms.HiveMetaStoreClient;
import com.amazonaws.athena.hms.HiveMetaStoreConf;
import com.amazonaws.services.lambda.runtime.Context;

import java.util.Set;

public class GetDatabaseNamesHandler extends BaseHMSHandler<GetDatabaseNamesRequest, GetDatabaseNamesResponse>
{
  public GetDatabaseNamesHandler(HiveMetaStoreConf conf, HiveMetaStoreClient client)
  {
    super(conf, client);
  }

  @Override
  public GetDatabaseNamesResponse handleRequest(GetDatabaseNamesRequest request, Context context)
  {
    HiveMetaStoreConf conf = getConf();
    try {
      context.getLogger().log("Connecting to HMS: " + conf.getMetastoreUri());
      HiveMetaStoreClient client = getClient();
      context.getLogger().log("Fetching all database names with filter: " + request.getFilter());
      Set<String> databases = client.getDatabaseNames(request.getFilter());
      context.getLogger().log("Fetched database names: " + (databases == null || databases.isEmpty() ? 0 : databases.size()));
      GetDatabaseNamesResponse response = new GetDatabaseNamesResponse();
      response.setDatabases(databases);
      return response;
    }
    catch (Exception e) {
      context.getLogger().log("Exception: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
