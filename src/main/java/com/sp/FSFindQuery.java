package com.sp;

import com.google.common.collect.Sets;
import java.util.Set;

import org.apache.hadoop.fs.Path;

/**
 * <code>FSFindQuery</code> represents a search query for <code>FSFind#find</code> method. It
 * enapsulates two types of data. First being the to-be-searched path (a directory) & the other type
 * is a set of paths which are marked as 'covered'. These paths act as hint for search algorithm, if
 * a path is marked as covered - algorithm does not explore it.
 */
public class FSFindQuery {

    private Path searchPath;
    private Set<Path> coveredPaths = Sets.newHashSet();

    private FSFindQuery(Path path) {
        this.searchPath = path;
    }

    /**
     * Helper method to create FSFindQuery with empty explored set.
     */
    public static FSFindQuery make(Path path) {
        return new FSFindQuery(path);
    }

    /**
     * Helper method to create an instance of FSFindQuery from FSFindResult. Client should pass the
     * FSFindResult from the last iteration. The explored path hint is passed from FSFindResult to
     * FSFindQuery.
     */
    public static FSFindQuery makeFromResult(Path path, FSFindResult result) {
        FSFindQuery query = new FSFindQuery(path);
        query.coveredPaths = Sets.newHashSet(result.explored());
        return query;
    }

    /**
     * Return the search path.
     */
    public Path searchPath() {
        return this.searchPath;
    }

    /**
     * Return true of path is marked covered/explored.
     */
    public boolean isCovered(Path path) {
        return this.coveredPaths.contains(path);
    }

}
