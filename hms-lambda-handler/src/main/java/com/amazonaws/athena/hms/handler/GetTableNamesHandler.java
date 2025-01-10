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

import com.amazonaws.athena.hms.GetTableNamesRequest;
import com.amazonaws.athena.hms.GetTableNamesResponse;
import com.amazonaws.athena.hms.HiveMetaStoreClient;
import com.amazonaws.athena.hms.HiveMetaStoreConf;
import com.amazonaws.services.lambda.runtime.Context;

import java.util.Set;

public class GetTableNamesHandler extends BaseHMSHandler<GetTableNamesRequest, GetTableNamesResponse>
{
  public GetTableNamesHandler(HiveMetaStoreConf conf, HiveMetaStoreClient client)
  {
    super(conf, client);
  }

  @Override
  public GetTableNamesResponse handleRequest(GetTableNamesRequest request, Context context)
  {
    HiveMetaStoreConf conf = getConf();
    HiveMetaStoreClient client = null;
    try {
      context.getLogger().log("Connecting to HMS: " + conf.getMetastoreUri());
      client = getClient();
      context.getLogger().log("Fetching all table names for DB: " + request.getDbName() + " with filter: " + request.getFilter());
      Set<String> tables = client.getTableNames(request.getDbName(), request.getFilter());
      context.getLogger().log("Fetched table names: " + (tables == null || tables.isEmpty() ? 0 : tables.size()));
      GetTableNamesResponse response = new GetTableNamesResponse();
      response.setTables(tables);
      return response;
    }
    catch (Exception e) {
      throw handleException(context, e);
    }
  }
}
