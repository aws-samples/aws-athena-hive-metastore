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
package com.amazonaws.athena.hms;

import com.amazonaws.athena.hms.handler.GetPartitionsHandler;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

import com.google.common.collect.Lists;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.thrift.TException;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestGetPartitionsHandler {

  @Test
  public void testHandleRequest() throws TException {
    HiveMetaStoreClient client = mock(HiveMetaStoreClient.class);
    Partition partition = new Partition();
    partition.setTableName("test");
    partition.setDbName("default");
    when(client.getPartitions(anyString(), anyString(), anyShort())).thenReturn(Lists.newArrayList(partition));
    GetPartitionsHandler handler = new GetPartitionsHandler(HiveMetaStoreConf.load(), client);
    GetPartitionsRequest request = new GetPartitionsRequest();
    request.setDbName("default");
    request.setTableName("test");
    Context context = mock(Context.class);
    when(context.getLogger()).thenReturn(mock(LambdaLogger.class));
    GetPartitionsResponse response = handler.handleRequest(request, context);
    assertNotNull(response);
    assertNotNull(response.getPartitions());
    assertEquals(1, response.getPartitions().size());
  }
}
