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
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.PageRequest;
import org.jobrunr.storage.nosql.cassandra.CassandraConnector;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

public class JobsByStateView extends AbstractCassandraObject {

    private final JobMapper jobMapper;

    public JobsByStateView(CassandraConnector cassandraConnector, JobMapper jobMapper) {
        super(cassandraConnector);
        this.jobMapper = jobMapper;

        createIfNotExists();
    }

    @Override
    public void createIfNotExists() {
        try {
            final CreateMaterializedView jobsByStateView = SchemaBuilder.createMaterializedView(keyspace, "jobs_by_state_view")
                    .ifNotExists()
                    .asSelectFrom(keyspace, "jobs")
                    .columns("id", "state", "insertedAt", "jobAsJson")
                    .whereColumn("id").isNotNull()
                    .whereColumn("state").isNotNull()
                    .whereColumn("insertedAt").isNotNull()
                    .withPartitionKey("state")
                    .withClusteringColumn("insertedAt")
                    .withClusteringColumn("id")
                    .withClusteringOrder("insertedAt", ClusteringOrder.ASC);

            getSession().execute(jobsByStateView.build());
        } catch (Exception e) {
            throw JobRunrException.shouldNotHappenException(e);
        }
    }

    public List<Job> getJobs(StateName state, PageRequest pageRequest) {
        final Select equalTo = QueryBuilder.selectFrom(keyspace, "jobs_by_state_view")
                .column("jobAsJson")
                .whereColumn("state").isEqualTo(literal(state.name()))
                .limit((int) (pageRequest.getOffset() + pageRequest.getLimit()))
                .orderBy("insertedAt", pageRequest.getOrder() == PageRequest.Order.ASC ? ClusteringOrder.ASC : ClusteringOrder.DESC);
        final ResultSet resultSet = getSession().execute(equalTo.build());
        return StreamSupport.stream(resultSet.spliterator(), false)
                .skip(pageRequest.getOffset())
                .map(row -> jobMapper.deserializeJob(row.getString(0)))
                .collect(Collectors.toList());
    }

    public List<Job> getJobs(StateName state, Instant updatedBefore, PageRequest pageRequest) {
        final Select equalTo = QueryBuilder.selectFrom(keyspace, "jobs_by_state_view")
                .column("jobAsJson")
                .whereColumn("state").isEqualTo(literal(state.name()))
                .whereColumn("insertedAt").isLessThanOrEqualTo(literal(JobsTable.toMicroSeconds(updatedBefore)))
                .limit((int) (pageRequest.getOffset() + pageRequest.getLimit()))
                .orderBy("insertedAt", pageRequest.getOrder() == PageRequest.Order.ASC ? ClusteringOrder.ASC : ClusteringOrder.DESC);
        final ResultSet resultSet = getSession().execute(equalTo.build());
        return StreamSupport.stream(resultSet.spliterator(), false)
                .skip(pageRequest.getOffset())
                .map(row -> jobMapper.deserializeJob(row.getString(0)))
                .collect(Collectors.toList());
    }

    public Long countJobs(StateName stateName) {
        final Select equalTo = QueryBuilder.selectFrom(keyspace, "jobs_by_state_view")
                .countAll()
                .whereColumn("state").isEqualTo(literal(stateName.name()));
        final ResultSet resultSet = getSession().execute(equalTo.build());
        final Row one = resultSet.one();
        if(one == null) {
            return 0L;
        }
        return one.getLong(0);
    }


}
