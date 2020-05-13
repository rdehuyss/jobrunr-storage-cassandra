package org.jobrunr.storage.nosql.cassandra.tables;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import org.jobrunr.JobRunrException;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.mappers.JobMapper;
import org.jobrunr.jobs.states.JobState;
import org.jobrunr.jobs.states.ScheduledState;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.ConcurrentJobModificationException;
import org.jobrunr.storage.nosql.cassandra.CassandraConnector;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

public class JobsTable extends AbstractCassandraObject {

    private final JobMapper jobMapper;
    private PreparedStatement preparedInsertStatement;
    private PreparedStatement preparedDeleteStatement;

    public JobsTable(CassandraConnector cassandraConnector, JobMapper jobMapper) {
        super(cassandraConnector);
        this.jobMapper = jobMapper;

        createIfNotExists();
        prepareStatements();
    }

    @Override
    public void createIfNotExists() {
        try {
            CreateTableWithOptions createJobsTable = SchemaBuilder.createTable(keyspace, "jobs")
                    .ifNotExists()
                    .withPartitionKey("id", DataTypes.UUID)
                    .withClusteringColumn("insertedAt", DataTypes.BIGINT)
                    .withColumn("version", DataTypes.INT)
                    .withColumn("state", DataTypes.TEXT)
                    .withColumn("jobSignature", DataTypes.TEXT)
                    .withColumn("scheduledAt", DataTypes.BIGINT)
                    .withColumn("jobAsJson", DataTypes.TEXT)
                    .withClusteringOrder("insertedAt", ClusteringOrder.ASC);

            getSession().execute(createJobsTable.build());
        } catch (Exception e) {
            throw JobRunrException.shouldNotHappenException(e);
        }
    }

    public int deleteJob(UUID id, int version, Instant insertedAt) {
        final BoundStatement boundDeleteStatement = getBoundedDeleteStatement(id, version, insertedAt);

        final ResultSet resultSet = getSession().execute(boundDeleteStatement);
        if (resultSet.wasApplied()) return 1;
        return 0;
    }

    public void insertJob(Job job) {
        job.setId(UUID.randomUUID());
        final BoundStatement boundStatement = getBoundedInsertStatement(job);
        getSession().execute(boundStatement);
    }

    public void updateJob(Job job) {
        JobState previousJobState = job.getJobState(-2);
        final BatchStatement batchStatement = BatchStatement
                .newInstance(BatchType.LOGGED)
                .add(getBoundedDeleteStatement(job.getId(), job.increaseVersion(), previousJobState.getCreatedAt()))
                .add(getBoundedInsertStatement(job));

        final ResultSet resultSet = getSession().execute(batchStatement);
        if (!resultSet.wasApplied()) {
            throw new ConcurrentJobModificationException(job.getId());
        }
    }

    private BoundStatement getBoundedInsertStatement(Job job) {
        Long scheduledAt = null;
        if(job.getState() == StateName.SCHEDULED) {
            scheduledAt = toMicroSeconds(((ScheduledState)job.getJobState()).getScheduledAt());
        }
        return preparedInsertStatement.bind()
                .setString("state", job.getState().name())
                .setUuid("id", job.getId())
                .setLong("insertedAt", toMicroSeconds(job.getUpdatedAt()))
                .setInt("version", job.getVersion())
                .setString("jobSignature", job.getJobSignature())
                .set("scheduledAt", scheduledAt, Long.class)
                .setString("jobAsJson", jobMapper.serializeJob(job));
    }

    private BoundStatement getBoundedDeleteStatement(UUID id, int version, Instant insertedAt) {
        return preparedDeleteStatement.bind()
                .setUuid("id", id)
                .setInt("version", version)
                .setLong("insertedAt", toMicroSeconds(insertedAt));
    }

    private void prepareStatements() {
        prepareInsertStatement();
        prepareDeleteStatement();
    }

    private void prepareInsertStatement() {
        final RegularInsert insertStatement = QueryBuilder.insertInto(keyspace, "jobs")
                .value("state", bindMarker())
                .value("id", bindMarker())
                .value("version", bindMarker())
                .value("insertedAt", bindMarker())
                .value("jobSignature", bindMarker())
                .value("scheduledAt", bindMarker())
                .value("jobAsJson", bindMarker());

        preparedInsertStatement = getSession().prepare(insertStatement.build());
    }

    private void prepareDeleteStatement() {
        final Delete deleteStatement = QueryBuilder.deleteFrom(keyspace, "jobs")
                .whereColumn("id").isEqualTo(bindMarker())
                .whereColumn("insertedAt").isEqualTo(bindMarker())
                .ifColumn("version").isEqualTo(bindMarker());

        preparedDeleteStatement = getSession().prepare(deleteStatement.build());
    }

    public static Long toMicroSeconds(Instant instant) {
        return ChronoUnit.MICROS.between(Instant.EPOCH, instant);
    }
}
