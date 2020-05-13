package org.jobrunr.storage.nosql.cassandra.tables;

import com.datastax.oss.driver.api.core.CqlSession;
import org.jobrunr.storage.nosql.cassandra.CassandraConnector;

public abstract class AbstractCassandraObject {

    protected final CassandraConnector cassandraConnector;
    protected final String keyspace;

    public AbstractCassandraObject(CassandraConnector cassandraConnector) {
        this.cassandraConnector = cassandraConnector;
        this.keyspace = cassandraConnector.getKeyspaceName();
    }

    abstract void createIfNotExists();

    protected CqlSession getSession() {
        return cassandraConnector.getSession();
    }

}
