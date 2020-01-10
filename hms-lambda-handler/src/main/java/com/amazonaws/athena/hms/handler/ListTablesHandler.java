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
package com.amazonaws.athena.hms.handler;

import com.amazonaws.athena.hms.HiveMetaStoreClient;
import com.amazonaws.athena.hms.HiveMetaStoreConf;
import com.amazonaws.athena.hms.ListTablesRequest;
import com.amazonaws.athena.hms.ListTablesResponse;
import com.amazonaws.athena.hms.PaginatedResponse;
import com.amazonaws.athena.hms.Paginator;
import com.amazonaws.services.lambda.runtime.Context;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ListTablesHandler extends BaseHMSHandler<ListTablesRequest, ListTablesResponse>
{
  public ListTablesHandler(HiveMetaStoreConf conf, HiveMetaStoreClient client)
  {
    super(conf, client);
  }

  private static class TablePaginator extends Paginator<Table>
  {
    private final Context context;
    private final ListTablesRequest request;
    private final HiveMetaStoreClient client;

    private TablePaginator(Context context, ListTablesRequest request, HiveMetaStoreClient client)
    {
      this.context = context;
      this.request = request;
      this.client = client;
    }

    @Override
    protected Collection<String> getNames() throws TException
    {
      context.getLogger().log("Fetching all table names for DB: " + request.getDbName()
          + " with filter: " + request.getFilter());
      return client.getTableNames(request.getDbName(), request.getFilter());
    }

    @Override
    protected List<Table> getEntriesByNames(List<String> names) throws TException
    {
      return client.getTablesByNames(request.getDbName(), names);
    }
  }

  @Override
  public ListTablesResponse handleRequest(ListTablesRequest request, Context context)
  {
    HiveMetaStoreConf conf = getConf();
    try {
      context.getLogger().log("Connecting to HMS: " + conf.getMetastoreUri());
      HiveMetaStoreClient client = getClient();
      ListTablesResponse response = new ListTablesResponse();
      TablePaginator paginator = new TablePaginator(context, request, client);
      PaginatedResponse<Table> paginatedResponse = paginator.paginateByNames(request.getNextToken(), request.getMaxSize());
      if (paginatedResponse != null) {
        response.setNextToken(paginatedResponse.getNextToken());
        List<Table> tables = paginatedResponse.getEntries();
        if (tables != null && !tables.isEmpty()) {
          TSerializer serializer = new TSerializer(getTProtocolFactory());
          List<String> jsonTableList = new ArrayList<>();
          for (Table table : tables) {
            jsonTableList.add(serializer.toString(table, StandardCharsets.UTF_8.name()));
          }
          response.setTables(jsonTableList);
          context.getLogger().log("Paginated response: entry size: " + jsonTableList.size()
              + ", nextToken: " + response.getNextToken());
        }
      }
      return response;
    }
    catch (RuntimeException e) {
      context.getLogger().log("Exception: " + e.getMessage());
      throw e;
    }
    catch (Exception e) {
      context.getLogger().log("Exception: " + e.getMessage());
      throw new RuntimeException(e);
    }
  }
}
