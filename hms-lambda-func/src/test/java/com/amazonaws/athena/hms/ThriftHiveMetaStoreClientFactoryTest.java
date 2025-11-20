/*-
 * #%L
 * hms-lambda-func
 * %%
 * Copyright (C) 2019 - 2025 Amazon Web Services
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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockedConstruction;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mockConstruction;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(MockitoJUnitRunner.class)
public class ThriftHiveMetaStoreClientFactoryTest {

  @Test
  public void testConstructor() {
    ThriftHiveMetaStoreClientFactory factory = new ThriftHiveMetaStoreClientFactory();
    assertNotNull(factory);
    assertTrue(factory instanceof HiveMetaStoreClientFactory);
  }

  @Test
  public void testGetConf() {
    ThriftHiveMetaStoreClientFactory factory = new ThriftHiveMetaStoreClientFactory();
    HiveMetaStoreConf conf = factory.getConf();
    assertNotNull(conf);
  }

  @Test
  public void testGetHiveMetaStoreClientFailure() {
    ThriftHiveMetaStoreClientFactory factory = new ThriftHiveMetaStoreClientFactory();
    try {
      HiveMetaStoreClient client = factory.getHiveMetaStoreClient();
    } catch (RuntimeException e) {
      assertEquals("Failed to create HiveMetaStoreClient", e.getMessage());
    }
  }

  @Test
  public void testGetHiveMetaStoreClientSuccess() {
    try (MockedConstruction<ThriftHiveMetaStoreClient> mockedConstruction =
         mockConstruction(ThriftHiveMetaStoreClient.class)) {
      ThriftHiveMetaStoreClientFactory factory = new ThriftHiveMetaStoreClientFactory();
      HiveMetaStoreClient client = factory.getHiveMetaStoreClient();
      assertNotNull(client);
      assertTrue(client instanceof ThriftHiveMetaStoreClient);
      assertEquals(1, mockedConstruction.constructed().size());
    }
  }

  @Test
  public void testGetHandlerProvider() {
    ThriftHiveMetaStoreClientFactory factory = new ThriftHiveMetaStoreClientFactory();
    HandlerProvider provider = factory.getHandlerProvider();
    assertNotNull(provider);
    assertTrue(provider instanceof ApiNameHandlerProvider);
  }
}
