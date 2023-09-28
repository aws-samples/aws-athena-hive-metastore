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

import com.amazonaws.athena.hms.AlterPartitionsRequest;
import com.amazonaws.athena.hms.AlterPartitionsResponse;
import com.amazonaws.athena.hms.HiveMetaStoreClient;
import com.amazonaws.athena.hms.HiveMetaStoreConf;
import com.amazonaws.services.lambda.runtime.Context;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.thrift.TDeserializer;

import java.util.ArrayList;
import java.util.List;

public class AlterPartitionsHandler extends BaseHMSHandler<AlterPartitionsRequest, AlterPartitionsResponse>
{
  public AlterPartitionsHandler(HiveMetaStoreConf conf, HiveMetaStoreClient client)
  {
    super(conf, client);
  }

  @Override
  public AlterPartitionsResponse handleRequest(AlterPartitionsRequest request, Context context)
  {
    HiveMetaStoreConf conf = getConf();
    HiveMetaStoreClient client = null;
    try {
      context.getLogger().log("Connecting to HMS: " + conf.getMetastoreUri());
      client = getClient();
      boolean isEmpty = request.getPartitionDescs() == null || request.getPartitionDescs().isEmpty();
      context.getLogger().log("Altering partitions: " +
          (isEmpty ? 0 : request.getPartitionDescs().size()));
      if (!isEmpty) {
        TDeserializer deserializer = new TDeserializer(getTProtocolFactory());
        List<Partition> partitionList = new ArrayList<>();
        for (String partitionDesc : request.getPartitionDescs()) {
          Partition partition = new Partition();
          deserializer.fromString(partition, partitionDesc);
          partitionList.add(partition);
        }
        client.alterPartitions(request.getDbName(), request.getTableName(), partitionList);
        context.getLogger().log("Altered partitions: " + partitionList.size());
      }
      return new AlterPartitionsResponse();
    }
    catch (Exception e) {
      throw handleException(context, e);
    }
  }
}
