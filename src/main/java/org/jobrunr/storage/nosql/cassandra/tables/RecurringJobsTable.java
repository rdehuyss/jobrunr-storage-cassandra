package org.jobrunr.storage.nosql.cassandra.tables;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import org.jobrunr.JobRunrException;
import org.jobrunr.jobs.RecurringJob;
import org.jobrunr.jobs.mappers.JobMapper;
import org.jobrunr.storage.nosql.cassandra.CassandraConnector;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;

public class RecurringJobsTable extends AbstractCassandraObject {

    private final JobMapper jobMapper;
    private PreparedStatement preparedInsertStatement;
    private PreparedStatement preparedDeleteStatement;

    public RecurringJobsTable(CassandraConnector cassandraConnector, JobMapper jobMapper) {
        super(cassandraConnector);
        this.jobMapper = jobMapper;

        createIfNotExists();
        prepareStatements();
    }

    @Override
    public void createIfNotExists() {
        try {
            CreateTableWithOptions createJobsTable = SchemaBuilder.createTable(keyspace, "recurring_jobs")
                    .ifNotExists()
                    .withPartitionKey("id", DataTypes.TEXT)
                    .withColumn("jobAsJson", DataTypes.TEXT);

            getSession().execute(createJobsTable.build());
        } catch (Exception e) {
            throw JobRunrException.shouldNotHappenException(e);
        }
    }

    public List<RecurringJob> getRecurringJobs() {
        final Select equalTo = QueryBuilder.selectFrom(keyspace, "recurring_jobs")
                .column("jobAsJson");
        final ResultSet resultSet = getSession().execute(equalTo.build());
        return StreamSupport.stream(resultSet.spliterator(), false)
                .map(row -> jobMapper.deserializeRecurringJob(row.getString(0)))
                .collect(Collectors.toList());
    }

    public RecurringJob saveRecurringJob(RecurringJob recurringJob) {
        final BoundStatement boundStatement = preparedInsertStatement.bind()
                .setString(0, recurringJob.getId())
                .setString(1, jobMapper.serializeRecurringJob(recurringJob));

        getSession().execute(boundStatement);
        return recurringJob;
    }

    public int deleteRecurringJob(String id) {
        final BoundStatement boundStatement = preparedDeleteStatement.bind()
                .setString("id", id);

        final ResultSet resultSet = getSession().execute(boundStatement);
        if (resultSet.wasApplied()) return 1;
        return 0;
    }

    private void prepareStatements() {
        prepareInsertStatement();
        prepareDeleteStatement();
    }

    private void prepareInsertStatement() {
        final RegularInsert insert = QueryBuilder.insertInto(keyspace, "recurring_jobs")
                .value("id", bindMarker())
                .value("jobAsJson", bindMarker());

        preparedInsertStatement = getSession().prepare(insert.build());
    }

    private void prepareDeleteStatement() {
        final Delete delete = QueryBuilder.deleteFrom(keyspace, "recurring_jobs")
                .whereColumn("id").isEqualTo(bindMarker())
                .ifExists();

        preparedDeleteStatement = getSession().prepare(delete.build());
    }
}
