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
import com.amazonaws.athena.hms.ListDatabasesRequest;
import com.amazonaws.athena.hms.ListDatabasesResponse;
import com.amazonaws.athena.hms.PaginatedResponse;
import com.amazonaws.athena.hms.Paginator;
import com.amazonaws.services.lambda.runtime.Context;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class ListDatabasesHandler extends BaseHMSHandler<ListDatabasesRequest, ListDatabasesResponse>
{
  public ListDatabasesHandler(HiveMetaStoreConf conf, HiveMetaStoreClient client)
  {
    super(conf, client);
  }

  private static class DatabasePaginator extends Paginator<Database>
  {
    private final Context context;
    private final ListDatabasesRequest request;
    private final HiveMetaStoreClient client;

    private DatabasePaginator(Context context, ListDatabasesRequest request, HiveMetaStoreClient client)
    {
      this.context = context;
      this.request = request;
      this.client = client;
    }

    @Override
    protected Collection<String> getNames() throws TException
    {
      context.getLogger().log("Fetching all database names with filter: " + request.getFilter());
      return client.getDatabaseNames(request.getFilter());
    }

    @Override
    protected List<Database> getEntriesByNames(List<String> names) throws TException
    {
      return client.getDatabasesByNames(names);
    }
  }

  @Override
  public ListDatabasesResponse handleRequest(ListDatabasesRequest request, Context context)
  {
    HiveMetaStoreConf conf = getConf();
    try {
      context.getLogger().log("Connecting to HMS: " + conf.getMetastoreUri());
      HiveMetaStoreClient client = getClient();
      ListDatabasesResponse response = new ListDatabasesResponse();
      DatabasePaginator paginator = new DatabasePaginator(context, request, client);
      PaginatedResponse<Database> paginatedResponse = paginator.paginateByNames(request.getNextToken(), request.getMaxSize());
      if (paginatedResponse != null) {
        response.setNextToken(paginatedResponse.getNextToken());
        List<Database> databases = paginatedResponse.getEntries();
        if (databases != null && !databases.isEmpty()) {
          TSerializer serializer = new TSerializer(getTProtocolFactory());
          List<String> jsonDatabaseList = new ArrayList<>();
          for (Database database : databases) {
            jsonDatabaseList.add(serializer.toString(database, StandardCharsets.UTF_8.name()));
          }
          response.setDatabases(jsonDatabaseList);
          context.getLogger().log("Paginated response: entry size: " + jsonDatabaseList.size()
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
