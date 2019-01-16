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
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.BasicQueryStats;
import com.facebook.presto.spi.QueryId;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.airlift.log.Logger;

import javax.sql.DataSource;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.NoSuchElementException;
import java.util.Properties;

/**
 * Using RDBMS to store/read query history. It should support most jdbc drivers.
 */
public class QueryHistorySQLStore
        implements QueryHistoryStore
{
    private static final Logger LOG = Logger.get(QueryHistorySQLStore.class);
    private static final ObjectMapper queryJsonParser;

    static {
        queryJsonParser = new ObjectMapper();
        queryJsonParser.registerModule(new Jdk8Module());
        queryJsonParser.registerModule(new JavaTimeModule());
        queryJsonParser.registerModule(new JodaModule());
        queryJsonParser.registerModule(new GuavaModule());
        queryJsonParser.registerModule(new PrestoQueryInfoModule());
        queryJsonParser.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        queryJsonParser.enableDefaultTyping();
    }

    public static ObjectMapper getQueryJsonParser()
    {
        return queryJsonParser;
    }

    // DDL
    public static final String CREATE_TABLE = "create table query_history (" +
            "id bigint unsigned not null auto_increment primary key, " +
            "cluster varchar(10) not null, " +
            "query_id varchar(100) not null, " +
            "query_state varchar(10) not null, " +
            "user varchar(50) not null, " +
            "source varchar(50), " +
            "catalog varchar(20), " +
            "create_time timestamp not null, " +
            "end_time timestamp, " +
            "query varchar(2000) not null, " +
            "query_info longtext not null);";
    private static final String SELECT_QUERY = "select query_info from query_history where query_id = ?";
    private static final String INSERT_QUERY = "insert into query_history(cluster, query_id, query_state, user, source," +
            " catalog, create_time, end_time, query, query_info) values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private Properties config;
    private DataSource dataSource;

    @Override
    public void init(Properties props)
    {
        config = props;
        HikariConfig dsConf = new HikariConfig(config);
        dsConf.setAutoCommit(false);
        dataSource = new HikariDataSource(dsConf);
    }

    @Override
    public QueryInfo getFullQueryInfo(QueryId queryId)
    {
        try {
            Connection connection = dataSource.getConnection();
            try {
                PreparedStatement statement = connection.prepareStatement(SELECT_QUERY);
                statement.setString(1, queryId.getId());
                try {
                    ResultSet resultSet = statement.executeQuery();
                    String json = resultSet.getString("query_info");
                    return queryJsonParser.readValue(json, QueryInfo.class);
                }
                finally {
                    statement.close();
                }
            }
            finally {
                connection.rollback();
            }
        }
        catch (Exception e) {
            LOG.error("SQL error while getting query " + queryId, e);
            throw new NoSuchElementException("Cannot get query for " + queryId);
        }
    }

    @Override
    public BasicQueryInfo getBasicQueryInfo(QueryId queryId)
    {
        QueryInfo fullQueryInfo = getFullQueryInfo(queryId);
        if (fullQueryInfo == null) {
            return null;
        }
        return new BasicQueryInfo(fullQueryInfo.getQueryId(), fullQueryInfo.getSession(),
                fullQueryInfo.getResourceGroupId(), fullQueryInfo.getState(), fullQueryInfo.getMemoryPool(),
                fullQueryInfo.isScheduled(), fullQueryInfo.getSelf(), fullQueryInfo.getQuery(),
                new BasicQueryStats(fullQueryInfo.getQueryStats()), fullQueryInfo.getErrorType(),
                fullQueryInfo.getErrorCode());
    }

    @Override
    public void saveFullQueryInfo(QueryInfo queryInfo)
    {
        try {
            Connection connection = dataSource.getConnection();
            try {
                PreparedStatement insertQuery = connection.prepareStatement(INSERT_QUERY);
                insertQuery.setString(1, getCluster());
                insertQuery.setString(2, queryInfo.getQueryId().getId());
                insertQuery.setString(3, queryInfo.getState().name());
                insertQuery.setString(4, queryInfo.getSession().getUser());
                insertQuery.setString(5, queryInfo.getSession().getSource().orElse(null));
                insertQuery.setString(6, queryInfo.getSession().getCatalog().orElse(null));
                insertQuery.setTimestamp(7, new Timestamp(queryInfo.getQueryStats().getCreateTime().getMillis()));
                insertQuery.setTimestamp(8, new Timestamp(queryInfo.getQueryStats().getEndTime().getMillis()));
                insertQuery.setString(9, queryInfo.getQuery());
                insertQuery.setClob(10, toJsonLongText(queryInfo, connection));
                insertQuery.execute();
                connection.commit();
            }
            finally {
                connection.rollback();
            }
        }
        catch (Exception e) {
            LOG.error("Failed to save query " + queryInfo);
        }
    }

    private Clob toJsonLongText(QueryInfo queryInfo, Connection connection) throws SQLException, JsonProcessingException
    {
        Clob jsonClob = connection.createClob();
        jsonClob.setString(1, queryJsonParser.writeValueAsString(queryInfo));
        return jsonClob;
    }

    @Override
    public void close() throws IOException
    {
        if (dataSource instanceof Closeable) {
            ((Closeable) dataSource).close();
        }
    }

    private static String getCluster()
    {
        return System.getProperty("CRITEO_ENV", "preprod") + "-" + System.getProperty("CRITEO_DC", "pa4");
    }
}
