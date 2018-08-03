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
package com.facebook.presto.hive.metastore.thrift;

import com.facebook.presto.hive.HiveErrorCode;
import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.net.HostAndPort;
import com.orbitz.consul.Consul;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.model.ConsulResponse;
import com.orbitz.consul.model.health.ServiceHealth;
import io.airlift.log.Logger;
import org.apache.thrift.transport.TTransportException;

import javax.inject.Inject;

import java.net.URI;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * This is like the standard StaticHiveCluster except it supports a dynamic metastore lookup based on URI format:
 * consul://consul-host:consul-port/service-name
 */
public class DiscoveryHiveCluster
        implements HiveCluster
{
    private static final Logger log = Logger.get(DiscoveryHiveCluster.class);
    private final HiveMetastoreClientFactory clientFactory;
    private final List<URI> unresolvedUris;
    private final Supplier<List<HostAndPort>> resolvedUriSupplier = Suppliers.memoizeWithExpiration(
            this::resolveUris,
            3,
            TimeUnit.MINUTES);

    @Inject
    public DiscoveryHiveCluster(DiscoveryHiveClusterConfig config, HiveMetastoreClientFactory clientFactory)
    {
        this.clientFactory = clientFactory;
        this.unresolvedUris = config.getMetastoreUris();
        //Trigger first resolution to fail fast.
        resolvedUriSupplier.get();
    }

    @Override
    public HiveMetastoreClient createMetastoreClient()
    {
        List<HostAndPort> resolvedUris = resolvedUriSupplier.get();
        Exception lastException = null;
        while (resolvedUris.size() > 0) {
            int index = ThreadLocalRandom.current().nextInt(resolvedUris.size());
            HostAndPort chosen = resolvedUris.remove(index);

            log.info("Connecting to metastore %s:%d", chosen.getHost(), chosen.getPort());
            try {
                return clientFactory.create(chosen);
            }
            catch (TTransportException e) {
                lastException = e;
            }
        }
        throw new PrestoException(HIVE_METASTORE_ERROR, "Failed connecting to Hive metastore: " + unresolvedUris, lastException);
    }

    private List<HostAndPort> resolveUris()
    {
        requireNonNull(unresolvedUris, "metastoreUris is null");
        checkArgument(!unresolvedUris.isEmpty(), "metastoreUris must specify at least one URI");
        List<HostAndPort> results = new LinkedList<HostAndPort>();
        for (URI uri : unresolvedUris) {
            String scheme = uri.getScheme();
            checkArgument(!isNullOrEmpty(scheme), "metastoreUri scheme is missing: %s", uri);
            if (scheme.equalsIgnoreCase("consul")) {
                try {
                    results.addAll(resolveUsingConsul(uri));
                }
                catch (Exception e) {
                    log.warn("Error resolving consul uri: " + uri, e);
                }
            }
            else {
                StaticHiveCluster.checkMetastoreUri(uri);
                results.add(HostAndPort.fromParts(uri.getHost(), uri.getPort()));
            }
        }
        if (results.size() == 0) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "Failed to resolve Hive metastore addresses: " + unresolvedUris);
        }
        return results;
    }

    private List<HostAndPort> resolveUsingConsul(URI consulUri)
    {
        log.info("Resolving consul uri : " + consulUri);
        // check arguments
        checkArgument(!isNullOrEmpty(consulUri.getHost()), "Unspecified consul host, please use consul://consul-host:consul-port/service-name");
        checkArgument(consulUri.getPort() != -1, "Unspecified consul port, please use consul://consul-host:consul-port/service-name");
        checkArgument(!isNullOrEmpty(consulUri.getPath()), "Unspecified consul service, please use consul://consul-host:consul-port/service-name");

        String consulHost = consulUri.getHost();
        String service = consulUri.getPath().substring(1);  //strip leading slash
        int consulPort = consulUri.getPort();

        HostAndPort hostAndPort = HostAndPort.fromParts(consulHost, consulPort);
        Consul consul = Consul.builder().withHostAndPort(hostAndPort).build();

        HealthClient healthClient = consul.healthClient();
        ConsulResponse<List<ServiceHealth>> result = healthClient.getHealthyServiceInstances(service);

        if (result == null) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, "No nodes found for " + service);
        }
        List<ServiceHealth> response = result.getResponse();
        return response.stream().map(uri -> {
            ServiceHealth chosen = response.get(0);
            String host = chosen.getNode().getNode();
            int port = chosen.getService().getPort();
            return HostAndPort.fromParts(host, port);
        }).collect(toList());
    }
}
