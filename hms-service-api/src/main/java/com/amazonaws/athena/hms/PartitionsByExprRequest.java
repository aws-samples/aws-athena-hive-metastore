/*-
 * #%L
 * hms-service-api
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
package com.amazonaws.athena.hms;

public class PartitionsByExprRequest extends ApiRequest
{
    private String dbName;
    private String tableName;
    private byte[] expr;
    private String defaultPartitionName;
    private short maxParts;

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

    public byte[] getExpr()
    {
        return expr;
    }

    public void setExpr(byte[] expr)
    {
        this.expr = expr;
    }

    public String getDefaultPartitionName()
    {
        return this.defaultPartitionName;
    }

    public void setDefaultPartitionName(String defaultPartitionName)
    {
        this.defaultPartitionName = defaultPartitionName;
    }

    public short getMaxParts()
    {
        return this.maxParts;
    }

    public void setMaxParts(short maxParts)
    {
        this.maxParts = maxParts;
    }

    public PartitionsByExprRequest withDbName(String dbName)
    {
        this.dbName = dbName;
        return this;
    }

    public PartitionsByExprRequest withTableName(String tableName)
    {
        this.tableName = tableName;
        return this;
    }

    public PartitionsByExprRequest withExpr(byte[] expr)
    {
        this.expr = expr;
        return this;
    }

    public PartitionsByExprRequest withDefaultPartitionName(String defaultPartitionName)
    {
        this.defaultPartitionName = defaultPartitionName;
        return this;
    }

    public PartitionsByExprRequest withMaxParts(short maxParts)
    {
        this.maxParts = maxParts;
        return this;
    }
}
