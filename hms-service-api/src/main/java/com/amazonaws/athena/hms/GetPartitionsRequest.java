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

public class GetPartitionsRequest extends ApiRequest
{
  private String dbName;
  private String tableName;
  private String partitionExpression;
  // default value "-1" means unlimited
  private short maxSize = -1;

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

  public String getPartitionExpression()
  {
    return partitionExpression;
  }

  public void setPartitionExpression(String partitionExpression)
  {
    this.partitionExpression = partitionExpression;
  }

  public short getMaxSize()
  {
    return maxSize;
  }

  public void setMaxSize(short maxSize)
  {
    this.maxSize = maxSize;
  }

  public GetPartitionsRequest withDbName(String dbName)
  {
    this.dbName = dbName;
    return this;
  }

  public GetPartitionsRequest withTableName(String tableName)
  {
    this.tableName = tableName;
    return this;
  }

  public GetPartitionsRequest withPartitionExpression(String partitionExpression)
  {
    this.partitionExpression = partitionExpression;
    return this;
  }

  public GetPartitionsRequest withMaxSize(short maxSize)
  {
    this.maxSize = maxSize;
    return this;
  }
}
