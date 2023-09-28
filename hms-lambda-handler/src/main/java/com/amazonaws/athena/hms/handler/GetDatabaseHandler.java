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

import com.amazonaws.athena.hms.GetDatabaseRequest;
import com.amazonaws.athena.hms.GetDatabaseResponse;
import com.amazonaws.athena.hms.HiveMetaStoreClient;
import com.amazonaws.athena.hms.HiveMetaStoreConf;
import com.amazonaws.services.lambda.runtime.Context;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.thrift.TSerializer;

import java.nio.charset.StandardCharsets;

public class GetDatabaseHandler extends BaseHMSHandler<GetDatabaseRequest, GetDatabaseResponse>
{
  public GetDatabaseHandler(HiveMetaStoreConf conf, HiveMetaStoreClient client)
  {
    super(conf, client);
  }

  @Override
  public GetDatabaseResponse handleRequest(GetDatabaseRequest request, Context context)
  {
    HiveMetaStoreConf conf = getConf();
    HiveMetaStoreClient client = null;
    try {
      context.getLogger().log("Connecting to HMS: " + conf.getMetastoreUri());
      client = getClient();
      context.getLogger().log("Fetching DB: " + request.getDbName());
      Database database = client.getDatabase(request.getDbName());
      context.getLogger().log("Fetched DB: " + database);
      GetDatabaseResponse response = new GetDatabaseResponse();
      if (database != null) {
        TSerializer serializer = new TSerializer(getTProtocolFactory());
        response.setDatabase(serializer.toString(database, StandardCharsets.UTF_8.name()));
      }
      return response;
    }
    catch (Exception e) {
      throw handleException(context, e);
    }
  }
}
