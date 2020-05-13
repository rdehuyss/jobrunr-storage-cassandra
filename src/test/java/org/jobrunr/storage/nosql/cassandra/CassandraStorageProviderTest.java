package org.jobrunr.storage.nosql.cassandra;

import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import org.assertj.core.api.Assertions;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.JobTestBuilder;
import org.jobrunr.jobs.mappers.JobMapper;
import org.jobrunr.storage.ConcurrentJobModificationException;
import org.jobrunr.storage.StorageProvider;
import org.jobrunr.storage.StorageProviderTest;
import org.jobrunr.utils.mapper.jackson.JacksonJsonMapper;
import org.junit.jupiter.api.Test;

import static org.jobrunr.utils.resilience.RateLimiter.Builder.rateLimit;

public class CassandraStorageProviderTest extends StorageProviderTest {

    private static CassandraConnector cassandraConnector;
    private static CassandraStorageProvider cassandraStorageProvider;

    @Override
    protected void cleanup() {
        final CassandraConnector cassandraConnector = getCassandraConnector();

        truncateTable("jobrunr", "jobs");
        truncateTable("jobrunr", "recurring_jobs");
        truncateTable("jobrunr", "background_job_servers");
        truncateTable("jobrunr", "job_stats");
    }

    @Override
    protected StorageProvider getStorageProvider() {
        if (cassandraStorageProvider == null) {
            cassandraStorageProvider = new CassandraStorageProvider(getCassandraConnector(), rateLimit().withoutLimits());
            cassandraStorageProvider.setJobMapper(new JobMapper(new JacksonJsonMapper()));
        }
        return cassandraStorageProvider;
    }

    private CassandraConnector getCassandraConnector() {
        if (cassandraConnector == null) {
            cassandraConnector = new CassandraConnector("127.0.0.1", 9042, "datacenter1", "jobrunr")
                    .createKeyspace(cassandraConnector -> SchemaBuilder.createKeyspace(cassandraConnector.getKeyspaceName())
                            .ifNotExists()
                            .withSimpleStrategy(1))
                    .connect();
        }
        return cassandraConnector;
    }

    private void truncateTable(String keyspace, String table) {
        try {
            cassandraConnector.getSession().execute(QueryBuilder.truncate(keyspace, table).build());
        } catch (Exception notImportant) {

        }
    }
}