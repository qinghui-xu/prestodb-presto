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
import com.google.common.net.HostAndPort;
import com.orbitz.consul.Consul;
import com.orbitz.consul.HealthClient;
import com.orbitz.consul.model.ConsulResponse;
import com.orbitz.consul.model.health.ServiceHealth;
import io.airlift.log.Logger;
import org.apache.thrift.transport.TTransportException;

import javax.inject.Inject;

import java.net.URI;
import java.util.Collections;
import java.util.List;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.isNullOrEmpty;

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
public class DiscoveryHiveCluster
        implements HiveCluster
{
    private static final Logger log = Logger.get(DiscoveryHiveCluster.class);
    private final HiveMetastoreClientFactory clientFactory;
    private final URI consulURI;

    @Inject
    public DiscoveryHiveCluster(DiscoveryHiveClusterConfig config, HiveMetastoreClientFactory clientFactory)
    {
        this.clientFactory = clientFactory;
        this.consulURI = config.getConsulUri();
    }

    @Override
    public HiveMetastoreClient createMetastoreClient()
    {
        String scheme = consulURI.getScheme();
        if (!scheme.equalsIgnoreCase("consul")) {
            throw new IllegalArgumentException("Invalid scheme, please use consul://consul-host:consul-port/service-name");
        }

        log.info("Resolving consul uri : " + consulURI);
        String consulHost = consulURI.getHost();
        String service = consulURI.getPath().substring(1);  //strip leading slash
        int consulPort = consulURI.getPort();

        // check that agentHost has scheme or not
        checkArgument(!isNullOrEmpty(consulHost), "Unspecified consul host, please use consul://consul-host:consul-port/service-name");
        checkArgument(consulPort != -1, "Unspecified consul port, please use consul://consul-host:consul-port/service-name");
        checkArgument(!isNullOrEmpty(service), "Unspecified consul service, please use consul://consul-host:consul-port/service-name");

        HostAndPort hostAndPort = HostAndPort.fromParts(consulHost, consulPort);
        Consul consul = Consul.builder().withHostAndPort(hostAndPort).build();

        HealthClient healthClient = consul.healthClient();
        ConsulResponse<List<ServiceHealth>> result = healthClient.getHealthyServiceInstances("hive-metastore");

        if (result == null) {
            throw new PrestoException(HiveErrorCode.HIVE_METASTORE_ERROR, "No nodes found for hive-metastore");
        }
        List<ServiceHealth> response = result.getResponse();
        Collections.shuffle(response);
        ServiceHealth chosen = response.get(0);
        String host = chosen.getNode().getNode();
        int port = chosen.getService().getPort();
        log.info("Connecting to metastore %s:%d", host, port);
        try {
            return clientFactory.create(HostAndPort.fromParts(host, port));
        }
        catch (TTransportException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, "Failed connecting to Hive metastore", e);
        }
    }
}
