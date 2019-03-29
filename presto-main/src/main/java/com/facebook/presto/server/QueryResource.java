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
package com.facebook.presto.server;

import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.StageId;
import com.facebook.presto.server.extension.ExtensionFactory;
import com.facebook.presto.server.extension.query.history.QueryHistoryStore;
import com.facebook.presto.spi.QueryId;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.connector.system.KillQueryProcedure.createKillQueryException;
import static com.facebook.presto.execution.QueryTracker.QUERY_HISTORY_EXTENSION_CONFIG_FILE;
import static com.facebook.presto.spi.StandardErrorCode.ADMINISTRATIVELY_KILLED;
import static java.util.Objects.requireNonNull;

/**
 * Manage queries scheduled on this node
 */
@Path("/v1/query")
public class QueryResource
{
    private static final Logger log = Logger.get(QueryResource.class);

    private final QueryManager queryManager;
    private Optional<? extends QueryHistoryStore> queryHistoryStore;

    @Inject
    public QueryResource(QueryManager queryManager)
    {
        this.queryManager = requireNonNull(queryManager, "queryManager is null");
        queryHistoryStore = loadQueryHistoryExtension();
    }

    private Optional<? extends QueryHistoryStore> loadQueryHistoryExtension()
    {
        Properties extensionProps;
        try {
            extensionProps = getExtensionConf();
        }
        catch (IOException e) {
            log.warn("Failed to load query extension config from " + QUERY_HISTORY_EXTENSION_CONFIG_FILE);
            return Optional.empty();
        }
        if (extensionProps == null) {
            return Optional.empty();
        }
        // The implementation class is defined as a property `com.facebook.presto.server.extension.query.history.QueryHistoryStore.impl`.
        String extensionImplClass = extensionProps.getProperty(QueryHistoryStore.class.getName() + ".impl");
        if (Strings.isNullOrEmpty(extensionImplClass)) {
            return Optional.empty();
        }
        return ExtensionFactory.INSTANCE.createExtension(extensionImplClass, extensionProps, QueryHistoryStore.class);
    }

    private static Properties getExtensionConf() throws IOException
    {
        File extensionPropsFile = new File(QUERY_HISTORY_EXTENSION_CONFIG_FILE);
        if (extensionPropsFile.exists()) {
            Properties config = new Properties();
            try (InputStream configResource = new FileInputStream(extensionPropsFile)) {
                config.load(configResource);
            }
            return config;
        }
        return null;
    }

    @GET
    public List<BasicQueryInfo> getAllQueryInfo(@QueryParam("state") String stateFilter)
    {
        QueryState expectedState = stateFilter == null ? null : QueryState.valueOf(stateFilter.toUpperCase(Locale.ENGLISH));
        ImmutableList.Builder<BasicQueryInfo> builder = new ImmutableList.Builder<>();
        for (BasicQueryInfo queryInfo : queryManager.getQueries()) {
            if (stateFilter == null || queryInfo.getState() == expectedState) {
                builder.add(queryInfo);
            }
        }
        return builder.build();
    }

    @GET
    @Path("{queryId}")
    public Response getQueryInfo(@PathParam("queryId") QueryId queryId)
    {
        requireNonNull(queryId, "queryId is null");

        try {
            QueryInfo queryInfo = queryManager.getFullQueryInfo(queryId);
            Response response = Response.ok(queryInfo).build();
            return response;
        }
        catch (NoSuchElementException e) {
            // Try to get query info (json) from history store
            return queryHistoryStore.map(store -> store.getFullQueryInfo(queryId))
                    .map(queryInfoJson -> Response.ok(queryInfoJson, MediaType.APPLICATION_JSON_TYPE))
                    .orElseGet(() -> Response.status(Status.GONE))
                    .build();
        }
    }

    @DELETE
    @Path("{queryId}")
    public void cancelQuery(@PathParam("queryId") QueryId queryId)
    {
        requireNonNull(queryId, "queryId is null");
        queryManager.cancelQuery(queryId);
    }

    @PUT
    @Path("{queryId}/killed")
    public Response killQuery(@PathParam("queryId") QueryId queryId, String message)
    {
        requireNonNull(queryId, "queryId is null");

        try {
            QueryState state = queryManager.getQueryState(queryId);

            // check before killing to provide the proper error code (this is racy)
            if (state.isDone()) {
                return Response.status(Status.CONFLICT).build();
            }

            queryManager.failQuery(queryId, createKillQueryException(message));

            // verify if the query was killed (if not, we lost the race)
            if (!ADMINISTRATIVELY_KILLED.toErrorCode().equals(queryManager.getQueryInfo(queryId).getErrorCode())) {
                return Response.status(Status.CONFLICT).build();
            }

            return Response.status(Status.OK).build();
        }
        catch (NoSuchElementException e) {
            return Response.status(Status.GONE).build();
        }
    }

    @DELETE
    @Path("stage/{stageId}")
    public void cancelStage(@PathParam("stageId") StageId stageId)
    {
        requireNonNull(stageId, "stageId is null");
        queryManager.cancelStage(stageId);
    }
}
