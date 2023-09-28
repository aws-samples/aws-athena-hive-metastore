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

import com.amazonaws.athena.hms.GetTableRequest;
import com.amazonaws.athena.hms.GetTableResponse;
import com.amazonaws.athena.hms.HiveMetaStoreClient;
import com.amazonaws.athena.hms.HiveMetaStoreConf;
import com.amazonaws.services.lambda.runtime.Context;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TSerializer;

import java.nio.charset.StandardCharsets;

public class GetTableHandler extends BaseHMSHandler<GetTableRequest, GetTableResponse>
{
  public GetTableHandler(HiveMetaStoreConf conf, HiveMetaStoreClient client)
  {
    super(conf, client);
  }

  @Override
  public GetTableResponse handleRequest(GetTableRequest request, Context context)
  {
    HiveMetaStoreConf conf = getConf();
    HiveMetaStoreClient client = null;
    try {
      context.getLogger().log("Connecting to HMS: " + conf.getMetastoreUri());
      client = getClient();
      context.getLogger().log("Fetching table " + request.getTableName() + " in DB: " + request.getDbName());
      Table table = client.getTable(request.getDbName(), request.getTableName());
      context.getLogger().log("Fetched table: " + request.getTableName());
      GetTableResponse response = new GetTableResponse();
      if (table != null) {
        TSerializer serializer = new TSerializer(getTProtocolFactory());
        response.setTableDesc(serializer.toString(table, StandardCharsets.UTF_8.name()));
      }
      return response;
    }
    catch (Exception e) {
      throw handleException(context, e);
    }
  }
}
