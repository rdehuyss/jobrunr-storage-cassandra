package org.jobrunr.storage.nosql.cassandra.tables;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateMaterializedView;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import org.jobrunr.JobRunrException;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.mappers.JobMapper;
import org.jobrunr.storage.JobNotFoundException;
import org.jobrunr.storage.nosql.cassandra.CassandraConnector;

import java.util.UUID;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

public class JobsByIdView extends AbstractCassandraObject {

    private final JobMapper jobMapper;

    public JobsByIdView(CassandraConnector cassandraConnector, JobMapper jobMapper) {
        super(cassandraConnector);
        this.jobMapper = jobMapper;

        createIfNotExists();
    }

    @Override
    public void createIfNotExists() {
        try {
            final CreateMaterializedView jobsByStateView = SchemaBuilder.createMaterializedView(keyspace, "jobs_by_id_view")
                    .ifNotExists()
                    .asSelectFrom(keyspace, "jobs")
                    .columns("id", "version", "insertedAt", "jobAsJson")
                    .whereColumn("id").isNotNull()
                    .whereColumn("version").isNotNull()
                    .whereColumn("insertedAt").isNotNull()
                    .withPartitionKey("id")
                    .withClusteringColumn("insertedAt")
                    .withClusteringColumn("version")
                    .withClusteringOrder("insertedAt", ClusteringOrder.ASC);

            getSession().execute(jobsByStateView.build());
        } catch (Exception e) {
            throw JobRunrException.shouldNotHappenException(e);
        }
    }

    public Job getJobById(UUID id) {
        final Select equalTo = QueryBuilder.selectFrom(keyspace, "jobs_by_id_view")
                .column("jobAsJson")
                .whereColumn("id").isEqualTo(literal(id));
        final ResultSet resultSet = getSession().execute(equalTo.build());
        final Row one = resultSet.one();
        if(one == null) throw new JobNotFoundException(id);
        return jobMapper.deserializeJob(one.getString(0));
    }

}