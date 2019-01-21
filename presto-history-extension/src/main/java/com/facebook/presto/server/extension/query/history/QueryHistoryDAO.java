/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.server.extension.query.history;

import com.facebook.presto.execution.QueryInfo;
import org.jdbi.v3.core.transaction.TransactionIsolationLevel;
import org.jdbi.v3.sqlobject.customizer.Bind;
import org.jdbi.v3.sqlobject.customizer.BindBean;
import org.jdbi.v3.sqlobject.statement.SqlQuery;
import org.jdbi.v3.sqlobject.statement.SqlUpdate;
import org.jdbi.v3.sqlobject.statement.UseRowMapper;
import org.jdbi.v3.sqlobject.transaction.Transaction;

public interface QueryHistoryDAO
{
    // DDL
    String CREATE_TABLE = "create table query_history (" +
            "id bigint unsigned not null auto_increment primary key, " +
            "cluster varchar(20) not null, " +
            "query_id varchar(100) not null, " +
            "query_state varchar(10) not null, " +
            "user varchar(50) not null, " +
            "source varchar(50), " +
            "catalog varchar(20), " +
            "create_time timestamp not null, " +
            "end_time timestamp, " +
            "query varchar(2000) not null, " +
            "query_info longtext not null);";

    @Transaction(TransactionIsolationLevel.READ_COMMITTED)
    @SqlUpdate("insert into query_history" +
            "(cluster, query_id, query_state, user, source, catalog, create_time, end_time, query, query_info) values" +
            "(:cluster, :queryId, :queryState, :user, :source, :catalog, :createTime, :endTime, :query, :queryInfo)")
    void insertQueryHistory(@BindBean QueryHistory queryHistory);

    @SqlQuery("select query_info from query_history where query_id = :query_id")
    @UseRowMapper(QueryInfoRowMapper.class)
    QueryInfo getQueryInfoByQueryId(@Bind("query_id") String queryId);
}
