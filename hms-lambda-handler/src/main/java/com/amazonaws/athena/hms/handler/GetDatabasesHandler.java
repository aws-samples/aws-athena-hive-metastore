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

import com.amazonaws.athena.hms.GetDatabasesRequest;
import com.amazonaws.athena.hms.GetDatabasesResponse;
import com.amazonaws.athena.hms.HiveMetaStoreClient;
import com.amazonaws.athena.hms.HiveMetaStoreConf;
import com.amazonaws.services.lambda.runtime.Context;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.thrift.TSerializer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class GetDatabasesHandler extends BaseHMSHandler<GetDatabasesRequest, GetDatabasesResponse>
{
  public GetDatabasesHandler(HiveMetaStoreConf conf, HiveMetaStoreClient client)
  {
    super(conf, client);
  }

  @Override
  public GetDatabasesResponse handleRequest(GetDatabasesRequest request, Context context)
  {
    HiveMetaStoreConf conf = getConf();
    try {
      context.getLogger().log("Connecting to HMS: " + conf.getMetastoreUri());
      HiveMetaStoreClient client = getClient();
      context.getLogger().log("Fetching all database objects with filter: " + request.getFilter());
      List<Database> databases = client.getDatabases(request.getFilter());
      context.getLogger().log("Fetched databases: " + (databases == null || databases.isEmpty() ? 0 : databases.size()));
      GetDatabasesResponse response = new GetDatabasesResponse();
      if (databases != null && !databases.isEmpty()) {
        TSerializer serializer = new TSerializer(getTProtocolFactory());
        List<String> jsonDatabaseList = new ArrayList<>();
        for (Database database : databases) {
          jsonDatabaseList.add(serializer.toString(database, StandardCharsets.UTF_8.name()));
        }
        response.setDatabaseObjects(jsonDatabaseList);
      }
      return response;
    }
    catch (Exception e) {
      context.getLogger().log("Exception: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
