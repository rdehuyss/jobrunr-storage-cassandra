package org.jobrunr.storage.nosql.cassandra.tables;

import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.metadata.schema.ClusteringOrder;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateMaterializedView;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import org.jobrunr.JobRunrException;
import org.jobrunr.jobs.JobDetails;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.nosql.cassandra.CassandraConnector;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilder.literal;
import static org.jobrunr.utils.JobUtils.getJobSignature;

public class JobsByJobSignatureView extends AbstractCassandraObject {

    public JobsByJobSignatureView(CassandraConnector cassandraConnector) {
        super(cassandraConnector);

        createIfNotExists();
    }

    @Override
    public void createIfNotExists() {
        try {
            final CreateMaterializedView jobsByStateView = SchemaBuilder.createMaterializedView(keyspace, "jobs_by_job_signature_view")
                    .ifNotExists()
                    .asSelectFrom(keyspace, "jobs")
                    .columns("id", "state", "jobSignature", "insertedAt", "jobAsJson")
                    .whereColumn("id").isNotNull()
                    .whereColumn("state").isNotNull()
                    .whereColumn("jobSignature").isNotNull()
                    .whereColumn("insertedAt").isNotNull()
                    .withPartitionKey("jobSignature")
                    .withClusteringColumn("insertedAt")
                    .withClusteringColumn("id")
                    .withClusteringOrder("insertedAt", ClusteringOrder.ASC);

            getSession().execute(jobsByStateView.build());
        } catch (Exception e) {
            throw JobRunrException.shouldNotHappenException(e);
        }
    }

    public boolean getJobByJobSignature(JobDetails jobDetails, StateName state) {
        final Select equalTo = QueryBuilder.selectFrom(keyspace, "jobs_by_job_signature_view")
                .countAll()
                .whereColumn("jobSignature").isEqualTo(literal(getJobSignature(jobDetails)))
                .whereColumn("state").isEqualTo(literal(state.name()))
                .allowFiltering();
        final ResultSet resultSet = getSession().execute(equalTo.build());
        final Row one = resultSet.one();
        return one.getLong(0) > 0;
    }

}