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
import com.facebook.presto.spi.QueryId;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.datatype.guava.GuavaModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.common.annotations.VisibleForTesting;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import io.airlift.log.Logger;
import org.jdbi.v3.core.Jdbi;
import org.jdbi.v3.sqlobject.SqlObjectPlugin;

import javax.sql.DataSource;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Using RDBMS to store/read query history. It should support most jdbc drivers.
 */
public class QueryHistorySQLStore
        implements QueryHistoryStore
{
    private static final Logger LOG = Logger.get(QueryHistorySQLStore.class);
    private static final ObjectMapper queryJsonParser;

    // All jdbc connection properties should be put under this namesapce, thus `jdbcUrl` should be `sql.jdbcUrl`.
    public static final String SQL_CONFIG_PREFIX = "sql.";
    public static final String PRESTO_CLUSTER_KEY = "presto.cluster";
    public static final String HISTORY_RETENTION_DAYS = "history.retention.days";
    public static final String HISTORY_RETENTION_CLEAN_SCHEDULE_HOURS = "history.retention.clean.schedule.hours";

    // Default value if property not set.
    private static final int DEFAULT_HISTORY_RETENION_DAYS = 15;
    private static final int DEFAULT_HISTORY_RETENTION_CLEAN_SCHEDULE_HOURS = 2;

    static {
        queryJsonParser = new ObjectMapper();
        queryJsonParser.registerModule(new Jdk8Module());
        queryJsonParser.registerModule(new JavaTimeModule());
        queryJsonParser.registerModule(new JodaModule());
        queryJsonParser.registerModule(new GuavaModule());
        queryJsonParser.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
        queryJsonParser.disable(MapperFeature.AUTO_DETECT_CREATORS,
                MapperFeature.AUTO_DETECT_FIELDS,
                MapperFeature.AUTO_DETECT_GETTERS,
                MapperFeature.AUTO_DETECT_IS_GETTERS,
                MapperFeature.AUTO_DETECT_SETTERS);
        queryJsonParser.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
    }

    private Properties config;
    private String cluster;
    private DataSource dataSource;
    private QueryHistoryDAO queryHistoryDAO;
    private List<String> basicQueryInfoProperties;

    private final ScheduledExecutorService houseKeepingScheduler = Executors.newScheduledThreadPool(1, r -> new Thread(r, "history-cleaner"));

    @Override
    public void init(Properties props)
    {
        config = props;
        cluster = config.getProperty(PRESTO_CLUSTER_KEY);
        requireNonNull(cluster, "You should define presto.cluster in your properties file.");
        dataSource = createDataSource(config);
        queryHistoryDAO = Jdbi.create(dataSource).installPlugin(new SqlObjectPlugin()).onDemand(QueryHistoryDAO.class);

        JavaType bqiType = queryJsonParser.getTypeFactory().constructType(BasicQueryInfo.class);
        List<BeanPropertyDefinition> bqiProperties = queryJsonParser.getSerializationConfig().introspect(bqiType).findProperties();
        basicQueryInfoProperties = bqiProperties.stream().map(property -> property.getName()).collect(Collectors.toList());

        int retentionDays = getRetentionDays();
        int retenionScheduleHours = getRetentionSchedulHours();
        houseKeepingScheduler.scheduleAtFixedRate(new HistoryRentionCleaner(retentionDays), 1, retenionScheduleHours, TimeUnit.HOURS);
    }

    private DataSource createDataSource(Properties config)
    {
        // Take all the sql configs to build a data source.
        Properties sqlConfig = new Properties();
        for (Map.Entry<Object, Object> entry : config.entrySet()) {
            if (entry.getKey().toString().startsWith(SQL_CONFIG_PREFIX)) {
                LOG.debug("History extension jdbc config: %s -> %s", entry.getKey(), entry.getValue());
                sqlConfig.put(entry.getKey().toString().substring(SQL_CONFIG_PREFIX.length()), entry.getValue());
            }
        }
        return new HikariDataSource(new HikariConfig(sqlConfig));
    }

    private int getRetentionSchedulHours()
    {
        return getPropertyInteger(HISTORY_RETENTION_CLEAN_SCHEDULE_HOURS, DEFAULT_HISTORY_RETENTION_CLEAN_SCHEDULE_HOURS);
    }

    private int getRetentionDays()
    {
        return getPropertyInteger(HISTORY_RETENTION_DAYS, DEFAULT_HISTORY_RETENION_DAYS);
    }

    private int getPropertyInteger(String name, int defaultValue)
    {
        String retentionDaysProp = config.getProperty(name);
        int retentionDays = defaultValue;
        if (retentionDaysProp == null) {
            LOG.info(name + " is undefined, use default value " + defaultValue);
        }
        else {
            try {
                retentionDays = Integer.parseUnsignedInt(retentionDaysProp);
                if (retentionDays <= 0) {
                    LOG.warn(name + " property <= 0, use default value " + defaultValue);
                    retentionDays = defaultValue;
                }
            }
            catch (Exception e) {
                LOG.warn(name + " property contains invalid integer, use default value " + defaultValue);
                retentionDays = defaultValue;
            }
        }
        return retentionDays;
    }

    @Override
    public String getFullQueryInfo(QueryId queryId)
    {
        try {
            return queryHistoryDAO.getQueryInfoByQueryId(queryId.getId());
        }
        catch (Exception e) {
            LOG.error("SQL error while getting query " + queryId, e);
            throw new RuntimeException("Cannot get query for " + queryId, e);
        }
    }

    @Override
    public String getBasicQueryInfo(QueryId queryId)
    {
        String jsonStr = getFullQueryInfo(queryId);
        if (jsonStr == null) {
            throw new NoSuchElementException("Cannot find QueryInfo from db: " + queryId);
        }
        ObjectNode queryInfoJson = null;
        try {
            queryInfoJson = (ObjectNode) queryJsonParser.reader().readTree(jsonStr);
            ObjectNode basicQueryInfoJson = queryInfoJson.retain(basicQueryInfoProperties);
            return basicQueryInfoJson.toString();
        }
        catch (IOException e) {
            throw new NoSuchElementException("Unparsable query Info " + jsonStr);
        }
    }

    @Override
    public void saveFullQueryInfo(QueryInfo queryInfo)
    {
        saveQueryHistory(queryInfo);
    }

    private boolean saveQueryHistory(QueryInfo queryInfo)
    {
        try {
            QueryHistory queryHistory = new QueryHistory(queryInfo, getCluster());
            queryHistoryDAO.insertQueryHistory(queryHistory);
            return true;
        }
        catch (Exception e) {
            LOG.error("Faield to save " + queryInfo, e);
            return false;
        }
    }

    public void clearHistoryOutOfRetention(LocalDateTime earliestHistory)
    {
        queryHistoryDAO.clearHistoryOutOfRetention(new Timestamp(earliestHistory.toEpochSecond(ZoneOffset.UTC) * 1000));
    }

    @Override
    public void close() throws IOException
    {
        if (dataSource instanceof Closeable) {
            ((Closeable) dataSource).close();
        }
    }

    /**
     * This is to be used only in test. It creates the table without the column compression attribute (this feature not yet available for tests).
     */
    @VisibleForTesting
    void createTable()
    {
        // Try to create the table if it does not exist.
        queryHistoryDAO.createQueryHistoryTable();
    }

    private String getCluster()
    {
        return cluster;
    }

    public static String serializeQueryInfo(QueryInfo queryInfo) throws IOException
    {
        return queryJsonParser.writeValueAsString(queryInfo);
    }

    public static QueryInfo deserializeQueryInfo(String json) throws IOException
    {
        return queryJsonParser.readValue(json, QueryInfo.class);
    }

    public static QueryInfo deserializeQueryInfo(InputStream inputStream) throws IOException
    {
        return queryJsonParser.readValue(inputStream, QueryInfo.class);
    }

    public static QueryInfo deserializeQueryInfo(Reader reader) throws IOException
    {
        return queryJsonParser.readValue(reader, QueryInfo.class);
    }

    class HistoryRentionCleaner
            implements Runnable
    {
        private final int retentionDays;

        HistoryRentionCleaner(int retentionDays)
        {
            this.retentionDays = retentionDays;
        }

        @Override
        public void run()
        {
            LocalDateTime now = LocalDateTime.now();
            LocalDateTime earliestHistory = now.minusDays(retentionDays);
            LOG.info("Remove query history before " + earliestHistory.toString());
            clearHistoryOutOfRetention(earliestHistory);
        }
    }
}
