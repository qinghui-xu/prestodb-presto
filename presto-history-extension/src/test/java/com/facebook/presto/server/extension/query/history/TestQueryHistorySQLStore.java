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

import ch.vorburger.mariadb4j.DB;
import com.facebook.presto.SessionRepresentation;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.QueryStats;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.joda.time.DateTime;
import org.testng.annotations.AfterGroups;
import org.testng.annotations.BeforeGroups;
import org.testng.annotations.Test;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.time.LocalDateTime;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.BlockedReason.WAITING_FOR_MEMORY;
import static com.facebook.presto.server.extension.query.history.QueryHistoryDAO.CREATE_TABLE_TEST;
import static com.facebook.presto.server.extension.query.history.QueryHistorySQLStore.PRESTO_CLUSTER_KEY;
import static com.facebook.presto.server.extension.query.history.QueryHistorySQLStore.SQL_CONFIG_PREFIX;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

public class TestQueryHistorySQLStore
{
    private static final Logger LOG = Logger.get(TestQueryHistorySQLStore.class);

    public static final String MARIA_DB_NAME = "PrestoQuery";

    private QueryInfo queryInfo = new QueryInfo(
            TEST_SESSION.getQueryId(),
            TEST_SESSION.toSessionRepresentation(),
            QueryState.FINISHED,
            new MemoryPoolId("reserved"),
            true,
            URI.create("1"),
            ImmutableList.of("2", "3"),
            "select * from test_table",
            new QueryStats(
                    DateTime.parse("1991-09-06T05:00:00.188Z"),
                    DateTime.parse("1991-09-06T05:01:59Z"),
                    DateTime.parse("1991-09-06T05:02Z"),
                    DateTime.parse("1991-09-06T06:00Z"),
                    Duration.valueOf("8m"),
                    Duration.valueOf("7m"),
                    Duration.valueOf("34m"),
                    Duration.valueOf("9m"),
                    Duration.valueOf("10m"),
                    Duration.valueOf("11m"),
                    Duration.valueOf("12m"),
                    13,
                    14,
                    15,
                    100,
                    17,
                    18,
                    34,
                    19,
                    20.0,
                    DataSize.valueOf("21GB"),
                    DataSize.valueOf("22GB"),
                    DataSize.valueOf("23GB"),
                    DataSize.valueOf("24GB"),
                    DataSize.valueOf("25GB"),
                    true,
                    Duration.valueOf("23m"),
                    Duration.valueOf("24m"),
                    Duration.valueOf("26m"),
                    true,
                    ImmutableSet.of(WAITING_FOR_MEMORY),
                    DataSize.valueOf("27GB"),
                    28,
                    DataSize.valueOf("29GB"),
                    30,
                    DataSize.valueOf("31GB"),
                    32,
                    DataSize.valueOf("33GB"),
                    ImmutableList.of(),
                    ImmutableList.of()),
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            ImmutableMap.of(),
            ImmutableSet.of(),
            ImmutableMap.of(),
            ImmutableSet.of(),
            Optional.empty(),
            false,
            "33",
            Optional.empty(),
            null,
            null,
            ImmutableList.of(),
            ImmutableSet.of(),
            Optional.empty(),
            false,
            Optional.empty());

    private DB mariaDB;

    @BeforeGroups("MariaDB-test")
    public void createMariaDB() throws Exception
    {
        LOG.info("Create MariaDB");
        ServerSocket socket = new ServerSocket(0);
        int mariaDBPort = socket.getLocalPort();
        socket.close();
        mariaDB = DB.newEmbeddedDB(mariaDBPort);
        mariaDB.start();
        mariaDB.createDB(MARIA_DB_NAME);
    }

    // Table does not exist upon the time the history store starts up, and will be created by the history store.
    // Also test using mysql driver to connect to mariaDB since we use mysql jdbc dependency.
    @Test(groups = "MariaDB-test")
    public void testSaveAndReadQueryInfoWithMariaDB() throws IOException
    {
        String mariaDbUrl = "jdbc:mysql://localhost:" + mariaDB.getConfiguration().getPort() + "/" + MARIA_DB_NAME;
        testSaveAndReadQueryInfo(mariaDbUrl);
    }

    @Test(groups = "MariaDB-test", dependsOnMethods = {"testSaveAndReadQueryInfoWithMariaDB"})
    public void testClearHistoryRetention() throws IOException
    {
        String mariaDbUrl = "jdbc:mysql://localhost:" + mariaDB.getConfiguration().getPort() + "/" + MARIA_DB_NAME;
        try (QueryHistorySQLStore historySQLStore = createHistorySQLStore(mariaDbUrl)) {
            // History is created from testSaveAndReadQueryInfoWithMariaDB
            String historyFromStore = historySQLStore.getFullQueryInfo(queryInfo.getQueryId());
            assertNotNull(historyFromStore);
            historySQLStore.clearHistoryOutOfRetention(LocalDateTime.of(1992, 1, 1, 0, 0));
            // History should be gone now.
            historyFromStore = historySQLStore.getFullQueryInfo(queryInfo.getQueryId());
            assertNull(historyFromStore);
        }
    }

    @AfterGroups("MariaDB-test")
    public void shutdownMariaDB() throws Exception
    {
        LOG.info("Shutdown MariaDB");
        mariaDB.stop();
    }

    // Table will be created by h2 datasource, using the init script in the jdbc url. The history store then just uses it.
    @Test
    public void testSaveAndReadQueryInfoWithH2() throws IOException
    {
        String h2dbUrl = "jdbc:h2:mem:query-store-test;INIT=" + CREATE_TABLE_TEST;
        testSaveAndReadQueryInfo(h2dbUrl);
    }

    private void testSaveAndReadQueryInfo(String jdbcUrl) throws IOException
    {
        QueryHistorySQLStore historySQLStore = createHistorySQLStore(jdbcUrl);
        try {
            historySQLStore.createTable();
            historySQLStore.saveFullQueryInfo(queryInfo);
            String historyFromStore = historySQLStore.getFullQueryInfo(queryInfo.getQueryId());
            assertNotNull(historyFromStore);
        }
        finally {
            historySQLStore.close();
        }
    }

    private QueryHistorySQLStore createHistorySQLStore(String jdbcUrl)
    {
        Properties storeConfig = new Properties();
        storeConfig.setProperty(SQL_CONFIG_PREFIX + "jdbcUrl", jdbcUrl);
        storeConfig.setProperty(PRESTO_CLUSTER_KEY, "dev");
        QueryHistorySQLStore historySQLStore = new QueryHistorySQLStore();
        historySQLStore.init(storeConfig);
        return historySQLStore;
    }

    @Test
    public void testQueryInfoSerDe() throws IOException
    {
        String serialized = QueryHistorySQLStore.serializeQueryInfo(queryInfo);
        QueryInfo deserialized = QueryHistorySQLStore.deserializeQueryInfo(serialized);
        verifyQueryStats(deserialized.getQueryStats(), queryInfo.getQueryStats());
        verifySession(deserialized.getSession(), queryInfo.getSession());
        assertEquals(deserialized.getQueryId(), queryInfo.getQueryId());
        assertEquals(deserialized.getResetSessionProperties(), queryInfo.getResetSessionProperties());
        assertEquals(deserialized.getQuery(), queryInfo.getQuery());
        assertEquals(deserialized.getState(), queryInfo.getState());
        assertEquals(deserialized.getSetSessionProperties(), queryInfo.getSetSessionProperties());
    }

    // equals is not defined for SessionRepresentation, so we have to compare its fields
    private void verifySession(SessionRepresentation actual, SessionRepresentation expected)
    {
        assertEquals(actual.getUser(), expected.getUser());
        assertEquals(actual.getPrincipal(), expected.getPrincipal());
        assertEquals(actual.getCatalog(), expected.getCatalog());
        assertEquals(actual.getSchema(), expected.getSchema());
        assertEquals(actual.getSource(), expected.getSource());
        assertEquals(actual.getClientTags(), expected.getClientTags());
    }

    // equals is not defined for QueryStats, so we have to compare its fields
    private void verifyQueryStats(QueryStats actual, QueryStats expected)
    {
        assertEquals(actual.getOperatorSummaries().size(), expected.getOperatorSummaries().size());
        assertEquals(actual.getCreateTime(), expected.getCreateTime());
        assertEquals(actual.getEndTime(), expected.getEndTime());
    }
}
