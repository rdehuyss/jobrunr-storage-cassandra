package org.jobrunr.storage.nosql.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;

import java.net.InetSocketAddress;
import java.util.function.Consumer;
import java.util.function.Function;

public class CassandraConnector {

    private final InetSocketAddress contactPoint;
    private final String dataCenter;
    private final String keyspace;
    private CqlSession session;
    private CreateKeyspace createKeyspace;

    public CassandraConnector(String node, Integer port, String dataCenter, String keyspace) {
        this(new InetSocketAddress(node, port), dataCenter, keyspace);
    }

    public CassandraConnector(InetSocketAddress address, String dataCenter, String keyspace) {
        contactPoint = address;
        this.dataCenter = dataCenter;
        this.keyspace = keyspace;
    }

    public CassandraConnector connect() {
        CqlSessionBuilder builder = CqlSession.builder();
        builder.addContactPoint(contactPoint);
        builder.withLocalDatacenter(dataCenter);
        session = builder.build();
        return this;
    }

    public CqlSession getSession() {
        return this.session;
    }

    public void close() {
        session.close();
    }

    public String getKeyspaceName() {
        return keyspace;
    }

    public CassandraConnector createKeyspace(Function<CassandraConnector, CreateKeyspace> cassandraConnectorConsumer) {
        createKeyspace = cassandraConnectorConsumer.apply(this);
        return this;
    }

    CreateKeyspace getCreateKeyspace() {
        return createKeyspace;
    }
}
