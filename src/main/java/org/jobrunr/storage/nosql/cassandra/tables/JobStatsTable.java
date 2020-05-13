package org.jobrunr.storage.nosql.cassandra.tables;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateTableWithOptions;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import com.datastax.oss.driver.api.querybuilder.update.Update;
import org.jobrunr.JobRunrException;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.nosql.cassandra.CassandraConnector;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;

public class JobStatsTable extends AbstractCassandraObject {

    public JobStatsTable(CassandraConnector cassandraConnector) {
        super(cassandraConnector);

        createIfNotExists();
    }

    @Override
    void createIfNotExists() {
        try {
            CreateTableWithOptions createJobStatsTable = SchemaBuilder.createTable(keyspace, "job_stats")
                    .ifNotExists()
                    .withPartitionKey("state", DataTypes.TEXT)
                    .withColumn("count", DataTypes.COUNTER);

            getSession().execute(createJobStatsTable.build());
        } catch (Exception e) {
            throw JobRunrException.shouldNotHappenException(e);
        }
    }

    public void publishJobStatCounter(StateName state, int amount) {
        // how to increment counter with prepared statement?
        final Update updateStatement = QueryBuilder.update(keyspace, "job_stats")
                .increment("count", literal(amount))
                .whereColumn("state").isEqualTo(literal(state.name()));

        getSession().execute(updateStatement.build());
    }

    public long getJobStatCounter(StateName state) {
        final Select selectCount = QueryBuilder.selectFrom(keyspace, "job_stats")
                .column("count")
                .whereColumn("state").isEqualTo(literal(state.name()));

        final ResultSet resultSet = getSession().execute(selectCount.build());
        final Row one = resultSet.one();
        if(one == null) {
            return 0L;
        }
        return one.getLong("count");
    }
}
