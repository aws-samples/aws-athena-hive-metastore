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

import java.util.List;

public class AlterPartitionsRequest extends ApiRequest
{
  private String dbName;
  private String tableName;
  private List<String> partitionDescs;

  public String getDbName()
  {
    return dbName;
  }

  public void setDbName(String dbName)
  {
    this.dbName = dbName;
  }

  public String getTableName()
  {
    return tableName;
  }

  public void setTableName(String tableName)
  {
    this.tableName = tableName;
  }

  public List<String> getPartitionDescs()
  {
    return partitionDescs;
  }

  public void setPartitionDescs(List<String> partitionDescs)
  {
    this.partitionDescs = partitionDescs;
  }

  public AlterPartitionsRequest withDbName(String dbName)
  {
    this.dbName = dbName;
    return this;
  }

  public AlterPartitionsRequest withTableName(String tableName)
  {
    this.tableName = tableName;
    return this;
  }

  public AlterPartitionsRequest withPartitionDescs(List<String> partitionDescs)
  {
    this.partitionDescs = partitionDescs;
    return this;
  }
}
