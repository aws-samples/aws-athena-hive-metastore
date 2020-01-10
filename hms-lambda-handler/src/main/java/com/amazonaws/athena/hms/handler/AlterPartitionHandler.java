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

import com.amazonaws.athena.hms.AlterPartitionRequest;
import com.amazonaws.athena.hms.AlterPartitionResponse;
import com.amazonaws.athena.hms.HiveMetaStoreClient;
import com.amazonaws.athena.hms.HiveMetaStoreConf;
import com.amazonaws.services.lambda.runtime.Context;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.thrift.TDeserializer;

public class AlterPartitionHandler extends BaseHMSHandler<AlterPartitionRequest, AlterPartitionResponse>
{
  public AlterPartitionHandler(HiveMetaStoreConf conf, HiveMetaStoreClient client)
  {
    super(conf, client);
  }

  @Override
  public AlterPartitionResponse handleRequest(AlterPartitionRequest request, Context context)
  {
    HiveMetaStoreConf conf = getConf();
    try {
      context.getLogger().log("Connecting to HMS: " + conf.getMetastoreUri());
      HiveMetaStoreClient client = getClient();
      context.getLogger().log("Altering partition: " + request.getPartitionDesc());
      TDeserializer deserializer = new TDeserializer(getTProtocolFactory());
      Partition partition = new Partition();
      deserializer.deserialize(partition, request.getPartitionDesc().getBytes());
      client.alterPartition(request.getDbName(), request.getTableName(), partition);
      context.getLogger().log("Altered partition for table " + request.getTableName() + " in DB " + request.getDbName());
      return new AlterPartitionResponse();
    }
    catch (Exception e) {
      context.getLogger().log("Exception: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
