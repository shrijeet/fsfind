package com.sp;


import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

/**
 * Some common filter implementations *
 */

public enum FSFindFilters implements PathFilter {

    ACCEPTS_ALL {
        @Override
        public boolean accept(Path path) {
            return true;
        }
    },

    MARKED_AS_DONT_DELETE {
        @Override
        public boolean accept(Path path) {
            if (path.toUri().getPath().contains(DONT_DELETE)) {
                return false;
            }
            return true;
        }
    };
    public static final String DONT_DELETE = "DONT_DELETE";

    public boolean accept(Path path) {
        return true;
    }
}
