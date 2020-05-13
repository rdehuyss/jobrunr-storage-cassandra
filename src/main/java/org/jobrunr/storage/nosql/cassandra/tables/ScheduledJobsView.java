package org.jobrunr.storage.nosql.cassandra.tables;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateMaterializedView;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import org.jobrunr.JobRunrException;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.mappers.JobMapper;
import org.jobrunr.storage.PageRequest;
import org.jobrunr.storage.nosql.cassandra.CassandraConnector;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

public class ScheduledJobsView extends AbstractCassandraObject {

    private final JobMapper jobMapper;

    public ScheduledJobsView(CassandraConnector cassandraConnector, JobMapper jobMapper) {
        super(cassandraConnector);
        this.jobMapper = jobMapper;

        createIfNotExists();
    }

    @Override
    public void createIfNotExists() {
        try {
            final CreateMaterializedView scheduledJobsView = SchemaBuilder.createMaterializedView(keyspace, "scheduled_jobs_view")
                    .ifNotExists()
                    .asSelectFrom(keyspace, "jobs")
                    .columns("id", "insertedAt", "scheduledAt", "jobAsJson")
                    .whereColumn("id").isNotNull()
                    .whereColumn("insertedAt").isNotNull()
                    .whereColumn("scheduledAt").isNotNull()
                    .withPartitionKey("id")
                    .withClusteringColumn("scheduledAt")
                    .withClusteringColumn("insertedAt")
                    .withClusteringOrder("scheduledAt", ClusteringOrder.ASC);

            getSession().execute(scheduledJobsView.build());
        } catch (Exception e) {
            throw JobRunrException.shouldNotHappenException(e);
        }
    }

    public List<Job> getScheduledJobs(Instant scheduledBefore, PageRequest pageRequest) {
        final Select scheduledJobs = QueryBuilder.selectFrom(keyspace, "scheduled_jobs_view")
                .column("jobAsJson")
                .whereColumn("scheduledAt").isLessThanOrEqualTo(literal(JobsTable.toMicroSeconds(scheduledBefore)))
                .limit((int) (pageRequest.getOffset() + pageRequest.getLimit()))
                .allowFiltering();
        final ResultSet resultSet = getSession().execute(scheduledJobs.build());

        return StreamSupport.stream(resultSet.spliterator(), false)
                .skip(pageRequest.getOffset())
                .map(row -> jobMapper.deserializeJob(row.getString(0)))
                .collect(Collectors.toList());
    }
}
