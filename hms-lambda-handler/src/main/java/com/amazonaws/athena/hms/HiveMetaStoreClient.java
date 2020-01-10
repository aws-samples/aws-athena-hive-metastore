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

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.DropPartitionsResult;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;
import java.util.Set;

// This list of client APIs is based on HiveMetaStoreService APIs
// TODO: This class will be refactored to include more APIs for Hive integration
public interface HiveMetaStoreClient
{
  boolean dbExists(String dbName) throws TException;

  boolean tableExists(String dbName, String tableName) throws TException;

  Database getDatabase(String dbName) throws TException;

  Set<String> getDatabaseNames(String filter) throws TException;

  List<Database> getDatabases(String filter) throws TException;

  List<Database> getDatabasesByNames(List<String> dbNames) throws TException;

  Set<String> getTableNames(String dbName, String filter) throws TException;

  List<Table> getTablesByNames(String dbName, List<String> tableNames) throws TException;

  boolean createDatabase(String name) throws TException;

  boolean createDatabase(String name,
                         String description,
                         String location,
                         Map<String, String> params)
      throws TException;

  boolean createDatabase(Database db) throws TException;

  boolean dropDatabase(String dbName) throws TException;

  boolean createTable(Table table) throws TException;

  boolean dropTable(String dbName, String tableName) throws TException;

  Table getTable(String dbName, String tableName) throws TException;

  Partition createPartition(Table table, List<String> values) throws TException;

  Partition addPartition(Partition partition) throws TException;

  void addPartitions(List<Partition> partitions) throws TException;

  List<String> getPartitionNames(String dbName, String tableName, short maxSize) throws TException;

  boolean dropPartition(String dbName, String tableName,
                        List<String> arguments) throws TException;

  List<Partition> getPartitions(String dbName, String tableName, short maxSize) throws TException;

  DropPartitionsResult dropPartitions(String dbName, String tableName,
                                      List<String> partNames) throws TException;

  List<Partition> getPartitionsByNames(String dbName, String tableName,
                                       List<String> names) throws TException;

  boolean alterTable(String dbName, String tableName, Table newTable)
      throws TException;

  void alterPartition(String dbName, String tableName,
                      Partition partition) throws TException;

  void alterPartitions(String dbName, String tableName,
                       List<Partition> partitions) throws TException;

  void appendPartition(String dbName, String tableName,
                       List<String> partitionValues) throws TException;
}
