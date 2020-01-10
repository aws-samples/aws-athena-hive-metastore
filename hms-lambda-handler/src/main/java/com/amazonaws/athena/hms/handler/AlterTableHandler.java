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

import com.amazonaws.athena.hms.AlterTableRequest;
import com.amazonaws.athena.hms.AlterTableResponse;
import com.amazonaws.athena.hms.HiveMetaStoreClient;
import com.amazonaws.athena.hms.HiveMetaStoreConf;
import com.amazonaws.services.lambda.runtime.Context;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TDeserializer;

public class AlterTableHandler extends BaseHMSHandler<AlterTableRequest, AlterTableResponse>
{
  public AlterTableHandler(HiveMetaStoreConf conf, HiveMetaStoreClient client)
  {
    super(conf, client);
  }

  @Override
  public AlterTableResponse handleRequest(AlterTableRequest request, Context context)
  {
    HiveMetaStoreConf conf = getConf();
    try {
      context.getLogger().log("Connecting to HMS: " + conf.getMetastoreUri());
      HiveMetaStoreClient client = getClient();
      context.getLogger().log("Altering table " + request.getTableName() + " in DB " + request.getDbName());
      TDeserializer deserializer = new TDeserializer(getTProtocolFactory());
      Table newTable = new Table();
      deserializer.deserialize(newTable, request.getTableDesc().getBytes());
      boolean successful = client.alterTable(request.getDbName(), request.getTableName(), newTable);
      context.getLogger().log("Altered table: " + successful);
      AlterTableResponse response = new AlterTableResponse();
      response.setSuccessful(successful);
      return response;
    }
    catch (Exception e) {
      context.getLogger().log("Exception: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
