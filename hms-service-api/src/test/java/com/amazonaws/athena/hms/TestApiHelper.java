/*-
 * #%L
 * hms-service-api
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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestApiHelper
{

  @Test
  public void testGetApiName()
  {
    ApiHelper helper = new ApiHelper();
    assertEquals("addPartition", helper.getApiName(AddPartitionRequest.class, AddPartitionResponse.class));
    assertEquals("addPartitions", helper.getApiName(AddPartitionsRequest.class, AddPartitionsResponse.class));
    assertEquals("alterPartition", helper.getApiName(AlterPartitionRequest.class, AlterPartitionResponse.class));
    assertEquals("alterPartitions", helper.getApiName(AlterPartitionsRequest.class, AlterPartitionsResponse.class));
    assertEquals("alterTable", helper.getApiName(AlterTableRequest.class, AlterTableResponse.class));
    assertEquals("appendPartition", helper.getApiName(AppendPartitionRequest.class, AppendPartitionResponse.class));
    assertEquals("createDatabaseFromDB", helper.getApiName(CreateDatabaseFromDBRequest.class, CreateDatabaseFromDBResponse.class));
    assertEquals("createDatabase", helper.getApiName(CreateDatabaseRequest.class, CreateDatabaseResponse.class));
    assertEquals("createPartition", helper.getApiName(CreatePartitionRequest.class, CreatePartitionResponse.class));
    assertEquals("createTable", helper.getApiName(CreateTableRequest.class, CreateTableResponse.class));
    assertEquals("dbExists", helper.getApiName(DbExistsRequest.class, DbExistsResponse.class));
    assertEquals("tableExists", helper.getApiName(TableExistsRequest.class, TableExistsResponse.class));
    assertEquals("dropDatabase", helper.getApiName(DropDatabaseRequest.class, DropDatabaseResponse.class));
    assertEquals("dropPartition", helper.getApiName(DropPartitionRequest.class, DropPartitionResponse.class));
    assertEquals("dropPartitions", helper.getApiName(DropPartitionsRequest.class, DropPartitionsResponse.class));
    assertEquals("dropTable", helper.getApiName(DropTableRequest.class, DropTableResponse.class));
    assertEquals("getDatabaseNames", helper.getApiName(GetDatabaseNamesRequest.class, GetDatabaseNamesResponse.class));
    assertEquals("getTableNames", helper.getApiName(GetTableNamesRequest.class, GetTableNamesResponse.class));
    assertEquals("getDatabase", helper.getApiName(GetDatabaseRequest.class, GetDatabaseResponse.class));
    assertEquals("getDatabases", helper.getApiName(GetDatabasesRequest.class, GetDatabasesResponse.class));
    assertEquals("getPartitionNames", helper.getApiName(GetPartitionNamesRequest.class, GetPartitionNamesResponse.class));
    assertEquals("getPartitionsByNames", helper.getApiName(GetPartitionsByNamesRequest.class, GetPartitionsByNamesResponse.class));
    assertEquals("getPartitions", helper.getApiName(GetPartitionsRequest.class, GetPartitionsResponse.class));
    assertEquals("getTable", helper.getApiName(GetTableRequest.class, GetTableResponse.class));
    assertEquals("getTables", helper.getApiName(GetTablesRequest.class, GetTablesResponse.class));
    assertEquals("listDatabases", helper.getApiName(ListDatabasesRequest.class, ListDatabasesResponse.class));
    assertEquals("listTables", helper.getApiName(ListTablesRequest.class, ListTablesResponse.class));
    assertEquals("listPartitions", helper.getApiName(ListPartitionsRequest.class, ListPartitionsResponse.class));
  }

  @Test
  public void testGetInvalidApiName()
  {
    ApiHelper helper = new ApiHelper ();
    assertNull(helper.getApiName(AddPartitionRequest.class, AddPartitionsResponse.class));
    assertNull(helper.getApiName(ListPartitionsRequest.class, GetPartitionsByNamesResponse.class));
  }

  @Test
  public void testGetRequestResponseClass()
  {
    ApiHelper helper = new ApiHelper();
    assertNotNull(helper.getRequestClass("listDatabases"));
    assertTrue(helper.getRequestClass("listDatabases").isAssignableFrom(ListDatabasesRequest.class));
    assertNotNull(helper.getResponseClass("listDatabases"));
    assertTrue(helper.getResponseClass("listDatabases").isAssignableFrom(ListDatabasesResponse.class));
  }
}
