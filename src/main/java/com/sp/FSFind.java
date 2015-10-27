package com.sp;

import java.io.IOException;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * *************************************************************
 * <p/>
 * An abstract class for searching a <code>FileSystem</code> and find files and directories that are
 * older than certain timestamp.
 * <p/>
 * Note: This class decides on modification time of the discovered paths.
 * <p/>
 * **************************************************************
 */
public abstract class FSFind {

    private static final PathFilter DEFAULT_FILTER = FSFindFilters.ACCEPTS_ALL;

    /**
     * {@code batch defaults to Integer.MAX_VALUE} and {@code filter defaults to
     * FSFind#DEFAULT_FILTER}
     *
     * @return list of paths meeting search criteria
     * @throws IOException
     * @see FSFind#find(FSFindQuery, long, int, org.apache.hadoop.fs.PathFilter)
     */
    public List<Path> find(FSFindQuery searchPath, long timestamp) throws IOException {
        return find(searchPath, timestamp, Integer.MAX_VALUE, DEFAULT_FILTER).candidates();
    }

    /**
     * {@code filter defaults to FSFind#DEFAULT_FILTER}
     *
     * @return an <code>FSFindResult</code> instance encapsulating the items and search queue
     * @throws IOException
     * @see FSFind#find(FSFindQuery, long, int, org.apache.hadoop.fs.PathFilter)
     */
    public FSFindResult find(FSFindQuery searchPath, long timestamp, int batch) throws IOException {
        return find(searchPath, timestamp, batch, DEFAULT_FILTER);
    }

    /**
     * {@code batch defaults to Integer.MAX_VALUE}
     *
     * @return list of paths meeting search criteria
     * @throws IOException
     * @see FSFind#find(FSFindQuery, long, int, org.apache.hadoop.fs.PathFilter)
     */
    public List<Path> find(FSFindQuery searchPath, long timestamp,
                           PathFilter filter) throws IOException {
        return find(searchPath, timestamp, Integer.MAX_VALUE, filter).candidates();
    }

    /**
     * Recursivly traverse a directory and find paths older than the given timestamp and stop if
     * number of paths found are more than the specified batch size. This is useful to operate on
     * paths in batches. The PathFilter rule applies to both files and directoties. If the directory
     * is rejected by path filter; it will be skipped all together i.e. files under it will not be
     * considered for time based check.
     *
     * @param searchPath path to begin the search
     * @param timestamp  only paths strictly older than this time would be returned
     * @param batchSize  the batch size
     * @param filter     a PathFilter to filter paths on additional criteria (other than timestamp)
     * @return an <code>FSFindResult</code> instance encapsulating the items and search queue
     */
    public abstract FSFindResult find(FSFindQuery searchPath, long timestamp, int batchSize,
                                      PathFilter filter) throws IOException;

}
