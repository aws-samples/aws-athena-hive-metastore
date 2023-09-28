/*-
 * #%L
 * hms-lambda-handler
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
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

import com.amazonaws.athena.hms.HiveMetaStoreClient;
import com.amazonaws.athena.hms.HiveMetaStoreConf;
import com.amazonaws.athena.hms.PartitionsByExprRequest;
import com.amazonaws.athena.hms.PartitionsByExprResponse;
import com.amazonaws.services.lambda.runtime.Context;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.thrift.TSerializer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class ListPartitionsByExprHandler extends BaseHMSHandler<PartitionsByExprRequest, PartitionsByExprResponse>
{
    public ListPartitionsByExprHandler(HiveMetaStoreConf conf, HiveMetaStoreClient client)
    {
        super(conf, client);
    }

    @Override
    public PartitionsByExprResponse handleRequest(PartitionsByExprRequest request, Context context)
    {
        HiveMetaStoreConf conf = getConf();
        HiveMetaStoreClient client = null;
        try {
            context.getLogger().log("Connecting to HMS: " + conf.getMetastoreUri());
            client = getClient();

            List<Partition> partitions = new ArrayList<Partition>();
            boolean hasUnKnownPartitions = client.listPartitionsByExpr(request.getDbName(), request.getTableName(), request.getExpr(),
                    request.getDefaultPartitionName(), request.getMaxParts(), partitions);

            context.getLogger().log(String.format("listPartitionsByExpr returns %d partitions , with unknown flag set to %b : ",
                    partitions.size(), hasUnKnownPartitions));

            PartitionsByExprResponse response = new PartitionsByExprResponse();
            response.setSuccessful(true);
            response.setHasUnKnownPartitions(hasUnKnownPartitions);

            List<String> jsonPartitionList = new ArrayList<>();
            if (!partitions.isEmpty()) {
                TSerializer serializer = new TSerializer(getTProtocolFactory());
                for (Partition partition : partitions) {
                    jsonPartitionList.add(serializer.toString(partition, StandardCharsets.UTF_8.name()));
                }
            }
            response.setPartitionDescs(jsonPartitionList);

            return response;
        }
        catch (Exception e) {
            throw handleException(context, e);
        }
    }
}
