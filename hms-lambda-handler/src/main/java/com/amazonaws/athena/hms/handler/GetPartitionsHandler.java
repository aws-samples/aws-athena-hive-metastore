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

import com.amazonaws.athena.hms.GetPartitionsRequest;
import com.amazonaws.athena.hms.GetPartitionsResponse;
import com.amazonaws.athena.hms.HiveMetaStoreClient;
import com.amazonaws.athena.hms.HiveMetaStoreConf;
import com.amazonaws.services.lambda.runtime.Context;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.thrift.TSerializer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class GetPartitionsHandler extends BaseHMSHandler<GetPartitionsRequest, GetPartitionsResponse>
{
  public GetPartitionsHandler(HiveMetaStoreConf conf, HiveMetaStoreClient client)
  {
    super(conf, client);
  }

  @Override
  public GetPartitionsResponse handleRequest(GetPartitionsRequest request, Context context)
  {
    HiveMetaStoreConf conf = getConf();
    try {
      context.getLogger().log("Connecting to HMS: " + conf.getMetastoreUri());
      HiveMetaStoreClient client = getClient();
      context.getLogger().log("Fetching partitions for DB: " + request.getDbName() + ", table: " + request.getTableName());
      List<Partition> partitionList =
          client.getPartitions(request.getDbName(), request.getTableName(), request.getMaxSize());
      context.getLogger().log("Fetched partitions: " + (partitionList == null || partitionList.isEmpty() ? 0 : partitionList.size()));
      GetPartitionsResponse response = new GetPartitionsResponse();
      if (partitionList != null && !partitionList.isEmpty()) {
        TSerializer serializer = new TSerializer(getTProtocolFactory());
        List<String> jsonPartitionList = new ArrayList<>();
        for (Partition partition : partitionList) {
          jsonPartitionList.add(serializer.toString(partition, StandardCharsets.UTF_8.name()));
        }
        response.setPartitions(jsonPartitionList);
      }
      return response;
    }
    catch (Exception e) {
      context.getLogger().log("Exception: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
