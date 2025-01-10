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

public class DropDatabaseRequest extends ApiRequest
{
  private String dbName;
  private boolean deleteData;
  private boolean cascade;

  public String getDbName()
  {
    return dbName;
  }

  public void setDbName(String dbName)
  {
    this.dbName = dbName;
  }

  public boolean isDeleteData()
  {
    return deleteData;
  }

  public void setDeleteData(boolean deleteData)
  {
    this.deleteData = deleteData;
  }

  public boolean isCascade()
  {
    return cascade;
  }

  public void setCascade(boolean cascade)
  {
    this.cascade = cascade;
  }

  public DropDatabaseRequest withDbName(String dbName)
  {
    this.dbName = dbName;
    return this;
  }

  public DropDatabaseRequest withDeleteData(boolean deleteData)
  {
    this.deleteData = deleteData;
    return this;
  }

  public DropDatabaseRequest withCascade(boolean cascade)
  {
    this.cascade = cascade;
    return this;
  }
}
