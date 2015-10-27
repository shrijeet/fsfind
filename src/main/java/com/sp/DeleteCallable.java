package com.sp;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * A <code>Callable</code> to perform delete operation on given path.
 */
public class DeleteCallable implements Callable<Boolean> {

    private FileSystem fs;
    private Path path;
    private CountDownLatch latch;

    /**
     * @param fs a filesystem instance
     * @param path a path to delete
     * @param latch countdown latch
     */
    public DeleteCallable(FileSystem fs, Path path, CountDownLatch latch) {
        this.fs = fs;
        this.path = path;
        this.latch = latch;
    }

    public Boolean call() throws Exception {
        try {
            return fs.delete(path, true);
        } finally {
            latch.countDown();
        }
    }
}
