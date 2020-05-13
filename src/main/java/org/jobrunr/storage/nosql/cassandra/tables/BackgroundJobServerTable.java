package org.jobrunr.storage.nosql.cassandra.tables;

import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.delete.Delete;
import com.datastax.oss.driver.api.querybuilder.insert.RegularInsert;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import org.jobrunr.JobRunrException;
import org.jobrunr.storage.BackgroundJobServerStatus;
import org.jobrunr.storage.ServerTimedOutException;
import org.jobrunr.storage.StorageException;
import org.jobrunr.storage.nosql.cassandra.CassandraConnector;

import java.time.Instant;
import java.util.List;
import java.util.stream.StreamSupport;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

public class BackgroundJobServerTable extends AbstractCassandraObject {

    private PreparedStatement preparedInsertStatement;
    private PreparedStatement preparedUpdateStatement;
    private PreparedStatement preparedSelectAllStatement;
    private PreparedStatement preparedSelectOneStatement;

    public BackgroundJobServerTable(CassandraConnector cassandraConnector) {
        super(cassandraConnector);

        createIfNotExists();
        prepareStatements();
    }

    @Override
    void createIfNotExists() {
        try {
            CreateTableWithOptions createJobsTable = SchemaBuilder.createTable(keyspace, "background_job_servers")
                    .ifNotExists()
                    .withPartitionKey("id", DataTypes.UUID)
                    .withClusteringColumn("firstHeartbeat", DataTypes.TIMESTAMP)
                    .withColumn("workerPoolSize", DataTypes.INT)
                    .withColumn("pollIntervalInSeconds", DataTypes.INT)
                    .withColumn("lastHeartbeat", DataTypes.TIMESTAMP)
                    .withColumn("running", DataTypes.BOOLEAN)
                    .withColumn("systemTotalMemory", DataTypes.BIGINT)
                    .withColumn("systemFreeMemory", DataTypes.BIGINT)
                    .withColumn("systemCpuLoad", DataTypes.DOUBLE)
                    .withColumn("processMaxMemory", DataTypes.BIGINT)
                    .withColumn("processFreeMemory", DataTypes.BIGINT)
                    .withColumn("processAllocatedMemory", DataTypes.BIGINT)
                    .withColumn("processCpuLoad", DataTypes.DOUBLE)
                    .withClusteringOrder("firstHeartbeat", ClusteringOrder.ASC);

            getSession().execute(createJobsTable.build());
        } catch (Exception e) {
            throw JobRunrException.shouldNotHappenException(e);
        }
    }

    public void announce(BackgroundJobServerStatus serverStatus) {
        final BoundStatement boundInsertStatement = preparedInsertStatement.bind()
                .setUuid(0, serverStatus.getId())
                .setInt(1, serverStatus.getWorkerPoolSize())
                .setInt(2, serverStatus.getPollIntervalInSeconds())
                .setInstant(3, serverStatus.getFirstHeartbeat())
                .setInstant(4, serverStatus.getLastHeartbeat())
                .setBoolean(5, serverStatus.isRunning())
                .setLong(6, serverStatus.getSystemTotalMemory())
                .setLong(7, serverStatus.getSystemFreeMemory())
                .setDouble(8, serverStatus.getSystemCpuLoad())
                .setLong(9, serverStatus.getProcessMaxMemory())
                .setLong(10, serverStatus.getProcessFreeMemory())
                .setLong(11, serverStatus.getProcessAllocatedMemory())
                .setDouble(12, serverStatus.getProcessCpuLoad());

        getSession().execute(boundInsertStatement);
    }

    public boolean signalServerAlive(BackgroundJobServerStatus serverStatus) {
        final BoundStatement boundSelectOneStatement = preparedSelectOneStatement.bind()
                .setUuid(0, serverStatus.getId())
                .setInstant(1, serverStatus.getFirstHeartbeat());
        final ResultSet resultSet = getSession().execute(boundSelectOneStatement);
        final Row one = resultSet.one();
        if(one == null) {
            throw new ServerTimedOutException(serverStatus, new StorageException("Background Job Server with id " + serverStatus.getId() + " is not found"));
        }

        final BoundStatement boundUpdateStatement = preparedUpdateStatement.bind()
                .setInstant(0, serverStatus.getLastHeartbeat())
                .setLong(1, serverStatus.getSystemFreeMemory())
                .setDouble(2, serverStatus.getSystemCpuLoad())
                .setLong(3, serverStatus.getProcessFreeMemory())
                .setLong(4, serverStatus.getProcessAllocatedMemory())
                .setDouble(5, serverStatus.getProcessCpuLoad())
                .setUuid(6, serverStatus.getId())
                .setInstant(7, serverStatus.getFirstHeartbeat());

        getSession().execute(boundUpdateStatement);
        return one.getBoolean(0);
    }

    public int removeAllWithLastHeartbeatOlderThan(Instant heartbeatOlderThan) {
        final List<BackgroundJobServerStatus> toDelete = getAll()
                .stream()
                .filter(serverStatus -> serverStatus.getLastHeartbeat().isBefore(heartbeatOlderThan))
                .collect(toList());
        if(!toDelete.isEmpty()) {
            // how to do this with prepared statement? No examples found...
            final Delete deleteStatement = QueryBuilder.deleteFrom(keyspace, "background_job_servers")
                    .whereColumn("id").in(toDelete.stream().map(serverStatus -> literal(serverStatus.getId())).collect(toSet()));
            getSession().execute(deleteStatement.build());
        }
        return toDelete.size();
    }

    public List<BackgroundJobServerStatus> getAll() {
        final BoundStatement boundStatement = preparedSelectAllStatement.bind();

        final ResultSet resultSet = getSession().execute(boundStatement);
        return StreamSupport.stream(resultSet.spliterator(), false)
                .map(this::toBackgroundJobServerStatus)
                .sorted(comparing(BackgroundJobServerStatus::getFirstHeartbeat))
                .collect(toList());
    }

    private BackgroundJobServerStatus toBackgroundJobServerStatus(Row row) {
        return new BackgroundJobServerStatus(
                row.getUuid("id"),
                row.getInt("workerPoolSize"),
                row.getInt("pollIntervalInSeconds"),
                row.getInstant("firstHeartbeat"),
                row.getInstant("lastHeartbeat"),
                row.getBoolean("running"),
                row.getLong("systemTotalMemory"),
                row.getLong("systemFreeMemory"),
                row.getDouble("systemCpuLoad"),
                row.getLong("processMaxMemory"),
                row.getLong("processFreeMemory"),
                row.getLong("processAllocatedMemory"),
                row.getDouble("processCpuLoad")
        );
    }

    private void prepareStatements() {
        prepareInsertStatement();
        prepareUpdateStatement();
        prepareSelectAllStatement();
        prepareSelectOneStatement();
    }

    private void prepareInsertStatement() {
        final RegularInsert insert = QueryBuilder.insertInto(keyspace, "background_job_servers")
                .value("id", bindMarker())
                .value("workerPoolSize", bindMarker())
                .value("pollIntervalInSeconds", bindMarker())
                .value("firstHeartbeat", bindMarker())
                .value("lastHeartbeat", bindMarker())
                .value("running", bindMarker())
                .value("systemTotalMemory", bindMarker())
                .value("systemFreeMemory", bindMarker())
                .value("systemCpuLoad", bindMarker())
                .value("processMaxMemory", bindMarker())
                .value("processFreeMemory", bindMarker())
                .value("processAllocatedMemory", bindMarker())
                .value("processCpuLoad", bindMarker());

        preparedInsertStatement = getSession().prepare(insert.build());
    }

    private void prepareUpdateStatement() {
        final Update update = QueryBuilder.update(keyspace, "background_job_servers")
                .setColumn("lastHeartbeat", bindMarker())
                .setColumn("systemFreeMemory", bindMarker())
                .setColumn("systemCpuLoad", bindMarker())
                .setColumn("processFreeMemory", bindMarker())
                .setColumn("processAllocatedMemory", bindMarker())
                .setColumn("processCpuLoad", bindMarker())
                .whereColumn("id").isEqualTo(bindMarker())
                .whereColumn("firstHeartbeat").isEqualTo(bindMarker());

        preparedUpdateStatement =  getSession().prepare(update.build());
    }

    private void prepareSelectAllStatement() {
        final Select select = QueryBuilder.selectFrom(keyspace, "background_job_servers")
                .all();

        preparedSelectAllStatement = getSession().prepare(select.build());
    }

    private void prepareSelectOneStatement() {
        final Select select = QueryBuilder.selectFrom(keyspace, "background_job_servers")
                .column("running")
                .whereColumn("id").isEqualTo(bindMarker())
                .whereColumn("firstHeartbeat").isEqualTo(bindMarker());

        preparedSelectOneStatement = getSession().prepare(select.build());
    }
}
