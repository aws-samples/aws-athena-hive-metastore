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

public class AlterDatabaseRequest extends ApiRequest
{
    private String dbName;
    private String dbDescription;

    public String getDbName()
    {
        return dbName;
    }

    public String getDbDescription()
    {
        return dbDescription;
    }

    public void setDbName(String dbName)
    {
        this.dbName = dbName;
    }

    public void setDbDescription(String dbDescription)
    {
        this.dbDescription = dbDescription;
    }

    public AlterDatabaseRequest withDbName(String dbName)
    {
        this.dbName = dbName;
        return this;
    }

    public AlterDatabaseRequest withDbDescription(String dbDescription)
    {
        this.dbDescription = dbDescription;
        return this;
    }
}
