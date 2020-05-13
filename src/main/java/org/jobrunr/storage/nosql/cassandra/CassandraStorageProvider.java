package org.jobrunr.storage.nosql.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import org.jobrunr.jobs.Job;
import org.jobrunr.jobs.JobDetails;
import org.jobrunr.jobs.RecurringJob;
import org.jobrunr.jobs.mappers.JobMapper;
import org.jobrunr.jobs.states.StateName;
import org.jobrunr.storage.AbstractStorageProvider;
import org.jobrunr.storage.BackgroundJobServerStatus;
import org.jobrunr.storage.JobStats;
import org.jobrunr.storage.Page;
import org.jobrunr.storage.PageRequest;
import org.jobrunr.storage.nosql.cassandra.tables.BackgroundJobServerTable;
import org.jobrunr.storage.nosql.cassandra.tables.JobStatsTable;
import org.jobrunr.storage.nosql.cassandra.tables.JobsByIdView;
import org.jobrunr.storage.nosql.cassandra.tables.JobsByJobSignatureView;
import org.jobrunr.storage.nosql.cassandra.tables.JobsByStateView;
import org.jobrunr.storage.nosql.cassandra.tables.JobsTable;
import org.jobrunr.storage.nosql.cassandra.tables.RecurringJobsTable;
import org.jobrunr.storage.nosql.cassandra.tables.ScheduledJobsView;
import org.jobrunr.utils.resilience.RateLimiter;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.jobrunr.utils.resilience.RateLimiter.Builder.rateLimit;
import static org.jobrunr.utils.resilience.RateLimiter.SECOND;

public class CassandraStorageProvider extends AbstractStorageProvider {

    private final CassandraConnector cassandraConnector;

    private JobsTable jobsTable;
    private JobsByStateView jobsByStateView;
    private JobsByIdView jobsByIdView;
    private JobsByJobSignatureView jobsByJobSignatureView;
    private ScheduledJobsView scheduledJobsView;

    private RecurringJobsTable recurringJobsTable;
    private JobStatsTable jobStatsTable;
    private BackgroundJobServerTable backgroundJobServersTable;

    public CassandraStorageProvider(CassandraConnector cassandraConnector) {
        this(cassandraConnector, rateLimit().at2Requests().per(SECOND));
    }

    public CassandraStorageProvider(CassandraConnector cassandraConnector, RateLimiter changeListenerNotificationRateLimit) {
        super(changeListenerNotificationRateLimit);
        this.cassandraConnector = cassandraConnector;
        createKeyspaceIfNecessary();
    }

    private void createKeyspaceIfNecessary() {
        final CreateKeyspace createKeyspace = cassandraConnector.getCreateKeyspace();
        if(createKeyspace != null) {
            final ResultSet resultSet = getSession().execute(createKeyspace.build());
            if(!resultSet.wasApplied()) throw new IllegalStateException("Could not create Keyspace " + cassandraConnector.getKeyspaceName());
        }
    }

    private CqlSession getSession() {
        return cassandraConnector.getSession();
    }

    @Override
    public void setJobMapper(JobMapper jobMapper) {
        jobsTable = new JobsTable(cassandraConnector, jobMapper);
        jobsByIdView = new JobsByIdView(cassandraConnector, jobMapper);
        jobsByStateView = new JobsByStateView(cassandraConnector, jobMapper);
        jobsByJobSignatureView = new JobsByJobSignatureView(cassandraConnector);
        scheduledJobsView = new ScheduledJobsView(cassandraConnector, jobMapper);
        recurringJobsTable = new RecurringJobsTable(cassandraConnector, jobMapper);
        jobStatsTable = new JobStatsTable(cassandraConnector);
        backgroundJobServersTable = new BackgroundJobServerTable(cassandraConnector);
    }

    @Override
    public void announceBackgroundJobServer(BackgroundJobServerStatus serverStatus) {
        backgroundJobServersTable.announce(serverStatus);
    }

    @Override
    public boolean signalBackgroundJobServerAlive(BackgroundJobServerStatus serverStatus) {
        return backgroundJobServersTable.signalServerAlive(serverStatus);
    }

    @Override
    public List<BackgroundJobServerStatus> getBackgroundJobServers() {
        return backgroundJobServersTable.getAll();
    }

    @Override
    public int removeTimedOutBackgroundJobServers(Instant heartbeatOlderThan) {
        return backgroundJobServersTable.removeAllWithLastHeartbeatOlderThan(heartbeatOlderThan);
    }

    @Override
    public Job save(Job job) {
        Job savedJob = saveJob(job);
        notifyOnChangeListeners();
        return savedJob;
    }

    @Override
    public int delete(UUID id) {
        final int amountOfJobsDeleted = deleteJob(id);
        notifyOnChangeListenersIf(amountOfJobsDeleted > 0);
        return amountOfJobsDeleted;
    }

    @Override
    public Job getJobById(UUID id) {
        return jobsByIdView.getJobById(id);
    }

    @Override
    public List<Job> save(List<Job> jobs) {
        jobs
                .forEach(this::saveJob);
        notifyOnChangeListenersIf(jobs.size() > 0);
        return jobs;
    }

    @Override
    public List<Job> getJobs(StateName state, Instant updatedBefore, PageRequest pageRequest) {
        return jobsByStateView.getJobs(state, updatedBefore, pageRequest);
    }

    @Override
    public List<Job> getScheduledJobs(Instant scheduledBefore, PageRequest pageRequest) {
        return scheduledJobsView.getScheduledJobs(scheduledBefore, pageRequest);
    }

    @Override
    public Long countJobs(StateName state) {
        return jobsByStateView.countJobs(state);
    }

    @Override
    public List<Job> getJobs(StateName state, PageRequest pageRequest) {
        return jobsByStateView.getJobs(state, pageRequest);
    }

    @Override
    public Page<Job> getJobPage(StateName state, PageRequest pageRequest) {
        long count = countJobs(state);
        if (count > 0) {
            List<Job> jobs = getJobs(state, pageRequest);
            return new Page<>(count, jobs, pageRequest);
        }
        return new Page<>(0, new ArrayList<>(), pageRequest);
    }

    @Override
    public int deleteJobs(StateName state, Instant updatedBefore) {
        AtomicInteger atomicInteger = new AtomicInteger();
        getJobs(state, updatedBefore, PageRequest.asc(0, 10000))
                .stream()
                .peek(job -> atomicInteger.incrementAndGet())
                .forEach(job -> deleteJob(job.getId()));
        final int amountOfJobsDeleted = atomicInteger.get();
        notifyOnChangeListenersIf(amountOfJobsDeleted > 0);
        return amountOfJobsDeleted;
    }

    @Override
    public boolean exists(JobDetails jobDetails, StateName state) {
        return jobsByJobSignatureView.getJobByJobSignature(jobDetails, state);
    }

    @Override
    public RecurringJob saveRecurringJob(RecurringJob recurringJob) {
        return recurringJobsTable.saveRecurringJob(recurringJob);
    }

    @Override
    public List<RecurringJob> getRecurringJobs() {
        return recurringJobsTable.getRecurringJobs();
    }

    @Override
    public int deleteRecurringJob(String id) {
        return recurringJobsTable.deleteRecurringJob(id);
    }

    @Override
    public JobStats getJobStats() {
        final Long awaitingState = countJobs(StateName.AWAITING);
        final Long scheduledState = countJobs(StateName.SCHEDULED);
        final Long enqueuedState = countJobs(StateName.ENQUEUED);
        final Long processingState = countJobs(StateName.PROCESSING);
        final Long failedState = countJobs(StateName.FAILED);
        final Long succeededState = countJobs(StateName.SUCCEEDED);

        return new JobStats(
                awaitingState + scheduledState + enqueuedState + processingState + failedState + succeededState,
                awaitingState,
                scheduledState,
                enqueuedState,
                processingState,
                failedState,
                succeededState + jobStatsTable.getJobStatCounter(StateName.SUCCEEDED),
                getRecurringJobs().size(),
                getBackgroundJobServers().size()
        );
    }

    @Override
    public void publishJobStatCounter(StateName state, int amount) {
        jobStatsTable.publishJobStatCounter(state, amount);
    }

    private Job saveJob(Job job) {
        if (job.getId() == null) {
            jobsTable.insertJob(job);
        } else {
            jobsTable.updateJob(job);
        }
        return job;
    }

    private int deleteJob(UUID id) {
        final Job job = getJobById(id);
        return jobsTable.deleteJob(job.getId(), job.getVersion(), job.getUpdatedAt());
    }
}
