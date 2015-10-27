package com.fsfind.retention;

import com.google.common.collect.Lists;

import com.fsfind.FSFind;
import com.fsfind.FSFindImpl;
import com.fsfind.FSFindQuery;
import com.fsfind.FSFindResult;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.GlobPattern;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Tool to enforce data retention
 */
public class DataRetention extends Command {
    static final String CONF_FILE = "conf_file";
    static final String POLICY = "policy";
    static final String HDFS_PATH = "hdfs_path";
    static final String NUM_DAYS = "num_days";
    static final String DELETE = "delete";
    private static final Logger LOG = Logger.getLogger(DataRetention.class);
    private static final int THREAD_POOL_SIZE = 5;
    private FileSystem fs;
    private FSFind fsFind;
    private ExecutorService deleteWorkers = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
    private boolean dryRun = true;

    /**
     * Constructor with <code>FileSystem</code> initialized based on
     * configuration found in classpath
     */
    public DataRetention() throws IOException {
        this(FileSystem.get(new Configuration()));
    }

    /**
     * Constructor with <code>FileSystem</code> initialized with instance passed
     * by the client
     *
     * @param fs a filesystem instance
     */
    public DataRetention(FileSystem fs) throws IOException {
        this.fs = fs;
        fsFind = new FSFindImpl(Boolean.TRUE, fs);
    }

    public static void main(String[] args) throws Exception {
        DataRetention retention = new DataRetention();
        System.exit(retention.doMain(args));
    }

    // meant for silencing the logger while unit testing
    static void silentLogger() {
        LOG.setLevel(Level.ERROR);
    }

    @Override
    @SuppressWarnings("static-access")
    public Options buildOptions() {
        Options options = new Options();
        options.addOption(OptionBuilder.withDescription(
                "Configuration defining data retention policy")
                .hasArg().isRequired(false).create(CONF_FILE));
        options.addOption(OptionBuilder.withDescription(
                "Name of the policy to apply from configuration, if not specified but" +
                        " configuration is given, all policies will be applied")
                .hasArg().isRequired(false).create(POLICY));
        options.addOption(OptionBuilder.withDescription(
                "HDFS path that needs to apply retention on.")
                .hasArg().isRequired(false).create(HDFS_PATH));
        options.addOption(OptionBuilder.withDescription(
                "Corresponding retention time period of given HDFS path.")
                .hasArg().isRequired(false).create(NUM_DAYS));
        options.addOption(OptionBuilder.withDescription(
                "Specify this if you don't want a dry run, " +
                        "unless this option is specified - data won't be deleted")
                .hasArg(false).isRequired(false).create(DELETE));
        return options;
    }

    @Override
    public int run(CommandLine cl) throws Exception {
        if (!isValidOption(cl)) {
            return FAILURE;
        }
        if (cl.hasOption(DELETE)) {
            LOG.info("Dry run has been turned off.");
            dryRun = false;
        } else {
            LOG.info("Doing dry run");
        }
        int totalDeleted = 0;
        if (cl.hasOption(CONF_FILE)) {
            totalDeleted = retentionByConfiguration(cl);
            LOG.info(String.format("Finished retention, deleted %d in total across all policies.",
                    totalDeleted));
        } else {
            totalDeleted = retentionByHdfsPath(cl);
            LOG.info(String.format(
                    "Finished retention, deleted %d in total under given HDFS path.",
                    totalDeleted));
        }
        fs.close();
        return SUCCESS;
    }

    /**
     * Apply retention on given <code>DataRetentionPolicy</code>. During dry run
     * batching is turned off
     *
     * @param policy data retention policy
     * @return total count of deleted paths covered by this policy
     */
    protected final int applyPolicy(DataRetentionPolicy policy) throws IOException {
        policy.validate();
        if (dryRun) {
            // turn off batching if doing dry run, else we might go into infinite loop since
            // search  module will keep returning the same paths again & again (because delete is
            // not executed in dry run)
            LOG.info("Since it's a dry run batching will be turned off");
            policy.turnOffBatching();
        }
        int totalDeleted = 0;
        Map<String, Integer> pathMapping = policy.getPathMapping();
        for (String pathPattern : pathMapping.keySet()) {
            totalDeleted += processPathEntry(pathPattern, policy);
        }
        return totalDeleted;
    }

    /**
     * Apply retention on given path
     *
     * @param pathPattern path from where to begin search from
     * @param policy      the policy which the path belongs to
     * @return total count of deleted paths under this base path
     */
    protected final int processPathEntry(String pathPattern, DataRetentionPolicy policy) throws
            IOException {
        List<Path> dirs = matchingDirectories(pathPattern);
        int totalDeleted = 0;
        for (Path dir : dirs) {
            LOG.info("Scanning " + dir);
            FSFindResult result = new FSFindResult();
            while (true) {
                FSFindQuery query = FSFindQuery.makeFromResult(dir, result);
                long purgeTime = purgeTime(policy.getPathMapping().get(pathPattern));
                result = fsFind.find(query, purgeTime, policy.getBatchSize());
                if (result.size() != 0) {
                    doDeletes(result); //blocks till all deletes finish
                    totalDeleted += result.size();
                    LOG.info(String.format("Deleted %d path(s) under %s", result.size(), dir));
                } else {
                    break;
                }
            }
        }
        LOG.info(String.format("Done with %s, deleted %d paths", pathPattern, totalDeleted));
        return totalDeleted;
    }

    /**
     * Issue async delete calls and wait for all async ops to finish. If running
     * in dry run mode, fake the deletes
     *
     * @param result the result of a <code>FSFind#find</code> operation
     */
    protected void doDeletes(FSFindResult result) {
        final CountDownLatch latch = new CountDownLatch(result.size());
        for (Path candidate : result.candidates()) {
            LOG.info("Deleting " + candidate);
            if (!dryRun) {
                deleteWorkers.submit(new DeleteCallable(fs, candidate, latch));
            } else {
                latch.countDown(); // fake the delete completion if dryrun.
            }
        }
        try {
            latch.await();
        } catch (InterruptedException e) {
            LOG.warn("Interrupted while waiting for deletes to finish", e);
        }
    }

    /**
     * Expand a glob patten and find the matching directories
     *
     * @param pathPattern the glob pattern
     * @return list of directories that matched the given glob pattern `
     */
    protected List<Path> matchingDirectories(String pathPattern) throws IOException {
        GlobPattern globPattern = new GlobPattern(pathPattern);
        List<Path> dirs = Lists.newArrayList();
        Path input = new Path(pathPattern);
        if (globPattern.hasWildcard()) {
            LOG.info("Expanding glob pattern " + input);
            for (FileStatus status : fs.globStatus(input)) {
                if (status.isDirectory()) {
                    dirs.add(status.getPath());
                }
            }
        } else {
            if (fs.exists(input)) {
                dirs.add(input);
            } else {
                // If directory doesn't exist we don't throw exception. This is debatable but
                // reasoning in favor of not throwing exception is that we don't want to abort
                // data retention process just because one path in policy configuration is
                // absent. The down side is that other than following warning, operator
                // has no way of knowing if they made a typo in config.
                LOG.warn(input + " doesn't exist!");
            }
        }
        return dirs;
    }

    /**
     * Calculate purge time as difference of current time and provided number of
     * days
     *
     * @param days number of days
     * @return difference between current time and given number of days in
     * millis
     */
    protected long purgeTime(int days) {
        return System.currentTimeMillis() - TimeUnit.DAYS.toMillis(days);
    }

    /**
     * Validate if input options are valid.
     *
     * @param cl list of arguments parsed from input options
     * @return true if input options are valid, otherwise false
     */
    boolean isValidOption(CommandLine cl) {
        boolean specifiedConfFile = cl.hasOption(CONF_FILE) || cl.hasOption(POLICY);
        boolean specifiedHdfsPath = cl.hasOption(HDFS_PATH) || cl.hasOption(NUM_DAYS);
        if (specifiedConfFile && specifiedHdfsPath) {
            System.err.println(String.format("Please provide either %s and %s, or %s and %s.",
                    CONF_FILE, POLICY, HDFS_PATH, NUM_DAYS));
            return false;
        }
        if (!cl.hasOption(CONF_FILE) && !cl.hasOption(HDFS_PATH)) {
            System.err.println("Both configuration and HDFS path are empty, exiting.");
            return false;
        }
        if (cl.hasOption(HDFS_PATH) && !cl.hasOption(NUM_DAYS)) {
            System.err.println("HDFS path is given but num_days is missing, exiting.");
            return false;
        }
        return true;
    }

    private int retentionByConfiguration(CommandLine cl) throws Exception {
        DataRetentionConfiguration configuration = new DataRetentionConfiguration();
        Map<String, DataRetentionPolicy> policyMap = configuration.load(cl.getOptionValue(CONF_FILE));
        int totalDeleted = 0;
        if (cl.hasOption(POLICY)) {
            String name = cl.getOptionValue(POLICY);
            if (!policyMap.containsKey(name)) {
                LOG.error("Configuration doesn't contain policy " + name);
                return FAILURE;
            } else {
                totalDeleted += applyPolicy(policyMap.get(name));
            }
        } else {
            for (String name : policyMap.keySet()) {
                LOG.info("Applying data retention on " + name);
                totalDeleted += applyPolicy(policyMap.get(name));
            }
        }
        return totalDeleted;
    }

    private int retentionByHdfsPath(CommandLine cl) throws Exception {
        return applyPolicy((new DataRetentionPolicyBuilder(
                cl.getOptionValue(HDFS_PATH), Integer.parseInt(cl.getOptionValue(NUM_DAYS)), 5000)).create());
    }

    // turn off dry run for testing
    void toggleDryRun(boolean value) {
        this.dryRun = value;
    }
}
