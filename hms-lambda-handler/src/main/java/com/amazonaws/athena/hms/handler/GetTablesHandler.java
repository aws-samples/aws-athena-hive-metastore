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

import com.amazonaws.athena.hms.GetTablesRequest;
import com.amazonaws.athena.hms.GetTablesResponse;
import com.amazonaws.athena.hms.HiveMetaStoreClient;
import com.amazonaws.athena.hms.HiveMetaStoreConf;
import com.amazonaws.services.lambda.runtime.Context;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TSerializer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class GetTablesHandler extends BaseHMSHandler<GetTablesRequest, GetTablesResponse>
{
  public GetTablesHandler(HiveMetaStoreConf conf, HiveMetaStoreClient client)
  {
    super(conf, client);
  }

  @Override
  public GetTablesResponse handleRequest(GetTablesRequest request, Context context)
  {
    HiveMetaStoreConf conf = getConf();
    HiveMetaStoreClient client = null;
    try {
      context.getLogger().log("Connecting to HMS: " + conf.getMetastoreUri());
      client = getClient();
      context.getLogger().log("Fetching tables for DB: " + request.getDbName());
      List<Table> tables = client.getTablesByNames(request.getDbName(), request.getTableNames());
      context.getLogger().log("Fetched tables: " + (tables == null || tables.isEmpty() ? 0 : tables.size()));
      GetTablesResponse response = new GetTablesResponse();
      if (tables != null && !tables.isEmpty()) {
        TSerializer serializer = new TSerializer(getTProtocolFactory());
        List<String> jsonTableList = new ArrayList<>();
        for (Table table : tables) {
          jsonTableList.add(serializer.toString(table, StandardCharsets.UTF_8.name()));
        }
        response.setTables(jsonTableList);
      }
      return response;
    }
    catch (Exception e) {
      throw handleException(context, e);
    }
  }
}
