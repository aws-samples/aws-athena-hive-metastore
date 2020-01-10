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

// An API interface to wrap Hive MetaStore APIs into Request and Response objects so that
// we could use a generic way to handle/serialize/deserialize them and we don't need to
// deal with different types and different numbers of parameters and returning variables
public interface HiveMetaStoreService
{
  // HMS API:  Partition addPartition(Partition partition) throws TException;
  AddPartitionResponse addPartition(AddPartitionRequest request) throws Exception;

  // HMS API: void addPartitions(List<Partition> partitions) throws TException;
  AddPartitionsResponse addPartitions(AddPartitionsRequest request) throws Exception;

  // HMS API:   void alterPartition(String dbName, String tableName,
  //                      Partition partition) throws TException;
  AlterPartitionResponse alterPartition(AlterPartitionRequest request) throws Exception;

  // HMS API:   void alterPartitions(String dbName, String tableName,
  //                       List<Partition> partitions) throws TException;
  AlterPartitionsResponse alterPartitions(AlterPartitionsRequest request) throws Exception;

  // HMS API:   boolean alterTable(String dbName, String tableName, Table newTable)
  //      throws TException;
  AlterTableResponse alterTable(AlterTableRequest request) throws Exception;

  // HMS API:   void appendPartition(String dbName, String tableName,
  //                       List<String> partitionValues) throws TException;
  AppendPartitionResponse appendPartition(AppendPartitionRequest request) throws Exception;

  // HMS API:   boolean createDatabase(String name) throws TException;
  CreateDatabaseFromDBResponse createDatabaseFromDB(CreateDatabaseFromDBRequest request) throws Exception;

  // HMS API:   boolean createDatabase(String name,
  //                         String description,
  //                         String location,
  //                         Map<String, String> params)
  //      throws TException;
  CreateDatabaseResponse createDatabase(CreateDatabaseRequest request) throws Exception;

  // HMS API:   Partition createPartition(Table table, List<String> values) throws TException;
  CreatePartitionResponse createPartition(CreatePartitionRequest request) throws Exception;

  // HMS API:   boolean createTable(Table table) throws TException;
  CreateTableResponse createTable(CreateTableRequest request) throws Exception;

  // HMS API:   boolean dbExists(String dbName) throws TException;
  DbExistsResponse dbExists(DbExistsRequest request) throws Exception;

  // HMS API:   boolean tableExists(String dbName, String tableName) throws TException;
  TableExistsResponse tableExists(TableExistsRequest request) throws Exception;

  // HMS API:   boolean dropDatabase(String dbName) throws TException;
  DropDatabaseResponse dropDatabase(DropDatabaseRequest request) throws Exception;

  // HMS API:   boolean dropPartition(String dbName, String tableName,
  //                        List<String> arguments) throws TException;
  DropPartitionResponse dropPartition(DropPartitionRequest request) throws Exception;

  // HMS API:   DropPartitionsResult dropPartitions(String dbName, String tableName,
  //                                      List<String> partNames) throws TException;
  DropPartitionsResponse dropPartitions(DropPartitionsRequest request) throws Exception;

  // HMS API:   boolean dropTable(String dbName, String tableName) throws TException;
  DropTableResponse dropTable(DropTableRequest request) throws Exception;

  // HMS API:   Set<String> getDatabaseNames(String filter) throws TException;
  GetDatabaseNamesResponse getDatabaseNames(GetDatabaseNamesRequest request) throws Exception;

  // HMS API:   List<Database> getDatabases(String filter) throws TException;
  GetDatabasesResponse getDatabases(GetDatabasesRequest request) throws Exception;

  // HMS API:   Set<String> getTables(String dbName, String filter) throws TException;
  GetTableNamesResponse getTableNames(GetTableNamesRequest request) throws Exception;

  // HMS API:   Database getDatabase(String dbName) throws TException;
  GetDatabaseResponse getDatabase(GetDatabaseRequest request) throws Exception;

  // HMS API:   List<String> getPartitionNames(String dbName,
  //                                 String tableName) throws TException;
  GetPartitionNamesResponse getPartitionNames(GetPartitionNamesRequest request) throws Exception;

  // HMS API:   List<Partition> getPartitionsByNames(String dbName, String tableName,
  //                                       List<String>names) throws TException;
  GetPartitionsByNamesResponse getPartitionsByNames(GetPartitionsByNamesRequest request) throws Exception;

  // HMS API:   List<Partition> getPartitions(String dbName, String tableName) throws TException;
  GetPartitionsResponse getPartitions(GetPartitionsRequest request) throws Exception;

  // HMS API:   Table getTable(String dbName, String tableName) throws TException;
  GetTableResponse getTable(GetTableRequest request) throws Exception;

  // HMS API:   List<Table> getTables(String dbName, List<String> tableNames) throws TException;
  GetTablesResponse getTables(GetTablesRequest request) throws Exception;

  // HMS API: List Database Objects with Pagination
  ListDatabasesResponse listDatabases(ListDatabasesRequest request) throws Exception;

  // HMS API: List Table Objects with Pagination
  ListTablesResponse listTables(ListTablesRequest request) throws Exception;

  // HMS API: List Partition Objects with Pagination
  ListPartitionsResponse listPartitions(ListPartitionsRequest request) throws Exception;
}
