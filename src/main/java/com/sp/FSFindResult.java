package com.sp;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.fs.Path;

/**
 * <code>FSFindResult</code> represents the result of a find operation. Along with the candidates
 * discovered during the search (which met the search criteria), it also contains a set of
 * directories which were completely explored during this search iteration. This knowledge can be
 * passed to the next iteration for optimizing the search.
 */
public class FSFindResult {

    private Set<Path> explored = Sets.newHashSet();
    private LinkedList<Path> candidates = Lists.newLinkedList();

    /**
     * @return the explored set
     */
    public Set<Path> explored() {
        return explored;
    }

    /**
     * @return the candidate that met the search criteria
     */
    public List<Path> candidates() {
        return candidates;
    }

    /**
     * @param path the path to be included in the candidate list
     */
    public void add(Path path) {
        this.candidates.add(path);
    }

    /**
     * Removes last 'n' candidates from current list
     *
     * @param n number of candidate to remove
     */
    public void removeLast(int n) {
        Preconditions.checkState(size() >= n, String.format("Have %d candidates" +
                ", cant remove %d more.", size(), n));
        candidates.subList(size() - n, size()).clear();
    }

    /**
     * Mark a path as explored
     *
     * @param path the path to be marked
     */
    public void markExplored(Path path) {
        this.explored.add(path);
    }

    /**
     * @return current size of the candidate list
     */
    public int size() {
        return candidates.size();
    }

    /**
     * @return the last included candidate path
     */
    public Path getLast() {
        return candidates.getLast();
    }

    @Override
    public String toString() {
        return "FSFindResult{" +
                "explored=" + explored +
                ", candidates=" + candidates +
                '}';
    }
}
