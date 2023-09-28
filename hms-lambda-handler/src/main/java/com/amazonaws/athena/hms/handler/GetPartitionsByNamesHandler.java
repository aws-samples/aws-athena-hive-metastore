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

import com.amazonaws.athena.hms.GetPartitionsByNamesRequest;
import com.amazonaws.athena.hms.GetPartitionsByNamesResponse;
import com.amazonaws.athena.hms.HiveMetaStoreClient;
import com.amazonaws.athena.hms.HiveMetaStoreConf;
import com.amazonaws.services.lambda.runtime.Context;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.thrift.TSerializer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class GetPartitionsByNamesHandler extends BaseHMSHandler<GetPartitionsByNamesRequest, GetPartitionsByNamesResponse>
{
  public GetPartitionsByNamesHandler(HiveMetaStoreConf conf, HiveMetaStoreClient client)
  {
    super(conf, client);
  }

  @Override
  public GetPartitionsByNamesResponse handleRequest(GetPartitionsByNamesRequest request, Context context)
  {
    HiveMetaStoreConf conf = getConf();
    HiveMetaStoreClient client = null;
    try {
      context.getLogger().log("Connecting to HMS: " + conf.getMetastoreUri());
      client = getClient();
      context.getLogger().log("Fetching partitions for DB: " + request.getDbName() + ", table: " + request.getTableName());
      List<Partition> partitionList =
          client.getPartitionsByNames(request.getDbName(), request.getTableName(), request.getNames());
      context.getLogger().log("Fetched partitions: " + (partitionList == null || partitionList.isEmpty() ? 0 : partitionList.size()));
      GetPartitionsByNamesResponse response = new GetPartitionsByNamesResponse();
      if (partitionList != null && !partitionList.isEmpty()) {
        TSerializer serializer = new TSerializer(getTProtocolFactory());
        List<String> jsonPartitionList = new ArrayList<>();
        for (Partition partition : partitionList) {
          jsonPartitionList.add(serializer.toString(partition, StandardCharsets.UTF_8.name()));
        }
        response.setPartitionDescs(jsonPartitionList);
      }
      return response;
    }
    catch (Exception e) {
      throw handleException(context, e);
    }
  }
}
