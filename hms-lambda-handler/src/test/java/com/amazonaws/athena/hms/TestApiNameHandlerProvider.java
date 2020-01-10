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

import com.amazonaws.athena.hms.handler.GetDatabasesHandler;
import com.amazonaws.athena.hms.handler.TableExistsHandler;

import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class TestApiNameHandlerProvider {

  @Test
  public void testProvide() {
    HiveMetaStoreConf conf = HiveMetaStoreConf.load();
    HiveMetaStoreClient client = mock(HiveMetaStoreClient.class);
    Map<String, HandlerContext> map = new ApiNameHandlerProvider(new ApiHelper()).provide(conf, client);
    assertNotNull(map);
    assertEquals(28, map.size());
    HandlerContext tableExists = map.get("tableExists");
    assertNotNull(tableExists);
    assertTrue(tableExists.getRequestClass().isAssignableFrom(TableExistsRequest.class));
    assertTrue(tableExists.getResponseClass().isAssignableFrom(TableExistsResponse.class));
    assertTrue(tableExists.getHandler() instanceof TableExistsHandler);
    HandlerContext allDbOjectsContext = map.get("getDatabases");
    assertNotNull(allDbOjectsContext);
    assertTrue(allDbOjectsContext.getRequestClass().isAssignableFrom(GetDatabasesRequest.class));
    assertTrue(allDbOjectsContext.getResponseClass().isAssignableFrom(GetDatabasesResponse.class));
    assertTrue(allDbOjectsContext.getHandler() instanceof GetDatabasesHandler);
  }
}
