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

import com.amazonaws.athena.hms.AddPartitionRequest;
import com.amazonaws.athena.hms.AddPartitionResponse;
import com.amazonaws.athena.hms.HiveMetaStoreClient;
import com.amazonaws.athena.hms.HiveMetaStoreConf;
import com.amazonaws.services.lambda.runtime.Context;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;

import java.nio.charset.StandardCharsets;

public class AddPartitionHandler extends BaseHMSHandler<AddPartitionRequest, AddPartitionResponse>
{
  public AddPartitionHandler(HiveMetaStoreConf conf, HiveMetaStoreClient client)
  {
    super(conf, client);
  }

  @Override
  public AddPartitionResponse handleRequest(AddPartitionRequest request, Context context)
  {
    HiveMetaStoreConf conf = getConf();
    try {
      context.getLogger().log("Connecting to HMS: " + conf.getMetastoreUri());
      HiveMetaStoreClient client = getClient();
      context.getLogger().log("Creating partition with desc: " + request.getPartitionDesc());
      TDeserializer deserializer = new TDeserializer(getTProtocolFactory());
      Partition partition = new Partition();
      deserializer.fromString(partition, request.getPartitionDesc());
      Partition resultingPartition = client.addPartition(partition);
      context.getLogger().log("Created partition: " + resultingPartition);
      AddPartitionResponse response = new AddPartitionResponse();
      if (resultingPartition != null) {
        TSerializer serializer = new TSerializer(getTProtocolFactory());
        response.setPartitionDesc(serializer.toString(resultingPartition, StandardCharsets.UTF_8.name()));
      }
      return response;
    }
    catch (Exception e) {
      context.getLogger().log("Exception: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
