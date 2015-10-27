package com.fsfind;

import com.google.common.base.Preconditions;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.FileNotFoundException;
import java.io.IOException;


public class FSFindImpl extends FSFind {

    public static final Log LOG = LogFactory.getLog(FSFindImpl.class);
    private boolean includeDirectories;
    private FileSystem fs;

    /**
     * Default constructor with <code>includeDirectories</code> flag set to
     * false
     *
     * @param fs the filesystem instace to access file metadata
     */
    public FSFindImpl(FileSystem fs) {
        this(false, fs);
    }

    /**
     * Constructor
     *
     * @param includeDirectories client should pass true if directories should
     *                           be included in 'find' output. When set true, If
     *                           while traversing a directory discovered that
     *                           all files directly underneath it are candidate
     *                           for inclusion, we include the directory itself
     *                           in the o/p, instead of individual files.
     * @param fs                 the filesystem instace to access file metadata
     */
    public FSFindImpl(boolean includeDirectories, FileSystem fs) {
        this.includeDirectories = includeDirectories;
        this.fs = fs;
    }

    @Override
    public FSFindResult find(FSFindQuery query, long timestamp,
                             int batchSize, PathFilter filter) throws IOException {
        FSFindResult result = new FSFindResult();
        Preconditions.checkState(fs.exists(query.searchPath()), String.format("%s does not " +
                "exist.", query.searchPath()));
        internalFind(query.searchPath(), query, timestamp, batchSize, filter, result);
        return result;
    }

    /**
     * The flow: 1. Verify the directory exists. 2. If directory was covered
     * during last search, skip it this time. 3. Check if path filter applies to
     * the directory, if filter rejects directory; don't check files underneath.
     * 4. Initialize the 'includedAllFiles' flag to true, the flag tracks the
     * fact whether or not we included all files as return candidates in the
     * directory being traversed. 5. Recrusively traverse the directory while
     * maintaining the 'includedAllFiles' flag. 6. If after finishing a
     * directory 'includedAllFiles' is still set as true, remove the individual
     * files and include the whole directory in return list. 7. If at any point
     * the candidate list gets bigger than the batch, bail out.
     *
     * Return <code>true</code> if we had to bail out early due to batch size
     * restriction. For all other reasons, return false.
     */
    private boolean internalFind(Path searchDir, FSFindQuery origQuery, long timestamp,
                                 int batchSize, PathFilter filter, FSFindResult result)
            throws IOException {
        FileStatus searchDirStatus;
        try {
            // At this point if dir doesn't exist; that certainly means it disappeared in the
            // middle of our execution flow. Just return.
            searchDirStatus = fs.getFileStatus(searchDir);
            searchDir = searchDirStatus.getPath(); // this will normalize the path (the uri part)
        } catch (FileNotFoundException e) {
            LOG.warn(String.format("%s can't be found, it must have been deleted after we " +
                    "started the search", searchDir));
            return false;
        }

        Preconditions.checkState(searchDirStatus.isDirectory(), "Expected a directory but found "
                + searchDir);

        /* optimization: if during last search we have already explored this path; dont redo */
        if (origQuery.isCovered(searchDir)) {
            result.markExplored(searchDir); // presrve this knowledge between runs
            return false;
        }

        /**
         * Check if this directory in itself is candidate for complete exclusion,
         * if yes - we skip checking files underneath.
         */
        REJECT_REASON reason = includePath(searchDirStatus, timestamp, filter);
        if (reason == REJECT_REASON.PATH_FILTER) {
            LOG.info("Directory was filtered by configured Pathfilter " + searchDir);
            result.markExplored(searchDir);
            return false;
        }

        // Recursively traverse the directory. NOTE: In a rare case listStatus could throw
        // FileNotFoundException. Its going to be rare because at the beginining we already
        // checked that directory exists. Between then and now there is no RPC call or nothing
        // expensive happens, so we have to be really unlucky for dir to get deleted between then
        // and now.
        FileStatus[] allFiles = fs.listStatus(searchDir);

        // if this flag is true it means all files and directories 'directly' under it were
        // included in the result set.
        boolean includedAllFiles = true;

        for (FileStatus status : allFiles) {
            if (status.isDirectory()) {
                if (internalFind(status.getPath(), origQuery, timestamp, batchSize, filter,
                        result)) {
                    /* while exploring a sub directory our batch got full,
                    so we bail early as well */
                    return true;
                } else if (result.size() == 0 ||
                        result.size() > 0 && !result.getLast().equals(status.getPath())) {
                    // Since the subdir was not included in the result, unset the all files flag.
                    includedAllFiles = false;
                }
            } else if (result.size() >= batchSize) {
                /* bail out */
                return true;
            } else if (includePath(status, timestamp, filter) == REJECT_REASON.NONE) {
                result.add(status.getPath());
            } else {
                includedAllFiles = false;
            }
        }


        if (allFiles.length == 0) {
            if (includeDirectories
                    && searchDirStatus.getModificationTime() < timestamp
                    && !currentSameAsOriginal(searchDir, origQuery.searchPath())) {
                result.add(searchDir);
            }
        } else if (
                result.size() > 0
                        && includeDirectories
                        && includedAllFiles
                        && !currentSameAsOriginal(searchDir, origQuery.searchPath())) {
            // Its odd that we don't check here if the sub directory's mtime is older than purge
            // time or not (unlike above). We don't check because when batching is on - a delete
            // operation could change the mtime of the directory if batch got full before
            // directory could be scanned fully.
            result.removeLast(allFiles.length);
            result.add(searchDir);
        }

        /* mark this explored to assist future search */
        result.markExplored(searchDir);
        return false;
    }

    private boolean currentSameAsOriginal(Path currentDir, Path origDir) {
        return currentDir.toUri().getPath().equals(origDir.toUri().getPath());
    }

    /* return REJECT_REASON.NONE only if mtime is older than our threshold and filter accepts
    the path */
    private REJECT_REASON includePath(FileStatus status, long purgeTime, PathFilter filter) {
        if (status.isDirectory() && !filter.accept(status.getPath())) {
            return REJECT_REASON.PATH_FILTER;
        } else if (status.getModificationTime() >= purgeTime) {
            return REJECT_REASON.NOT_OLD_ENOUGH;
        } else {
            return REJECT_REASON.NONE;
        }
    }

    private enum REJECT_REASON {
        NONE, // accepted!
        NOT_OLD_ENOUGH,
        PATH_FILTER,
    }
}
