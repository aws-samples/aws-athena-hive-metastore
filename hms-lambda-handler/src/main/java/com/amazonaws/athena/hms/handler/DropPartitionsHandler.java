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

import com.amazonaws.athena.hms.DropPartitionsRequest;
import com.amazonaws.athena.hms.DropPartitionsResponse;
import com.amazonaws.athena.hms.HiveMetaStoreClient;
import com.amazonaws.athena.hms.HiveMetaStoreConf;
import com.amazonaws.services.lambda.runtime.Context;
import org.apache.hadoop.hive.metastore.api.DropPartitionsResult;
import org.apache.thrift.TSerializer;

import java.nio.charset.StandardCharsets;

public class DropPartitionsHandler extends BaseHMSHandler<DropPartitionsRequest, DropPartitionsResponse>
{
  public DropPartitionsHandler(HiveMetaStoreConf conf, HiveMetaStoreClient client)
  {
    super(conf, client);
  }

  @Override
  public DropPartitionsResponse handleRequest(DropPartitionsRequest request, Context context)
  {
    HiveMetaStoreConf conf = getConf();
    try {
      context.getLogger().log("Connecting to HMS: " + conf.getMetastoreUri());
      HiveMetaStoreClient client = getClient();
      context.getLogger().log("Dropping partitions for DB " + request.getDbName() + " table " + request.getTableName());
      DropPartitionsResult result = client.dropPartitions(request.getDbName(), request.getTableName(), request.getPartNames());
      context.getLogger().log("Dropped partitions: " + result);
      DropPartitionsResponse response = new DropPartitionsResponse();
      if (result != null) {
        TSerializer serializer = new TSerializer(getTProtocolFactory());
        response.setResult(serializer.toString(result, StandardCharsets.UTF_8.name()));
      }
      return response;
    }
    catch (Exception e) {
      context.getLogger().log("Exception: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
