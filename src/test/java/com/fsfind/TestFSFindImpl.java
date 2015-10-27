package com.fsfind;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestFSFindImpl {

    private FileSystem localFS;
    private List<File> tmpDirs = new ArrayList<File>();
    private FSFindTestUtil findTestUtil = new FSFindTestUtil();

    @BeforeClass(groups = { "unit" })
    public void setUp() throws Exception {
        Configuration conf = new Configuration();
        localFS = FileSystem.getLocal(conf);
    }

    @AfterClass(groups = { "unit" })
    public void tearDown() throws Exception {
        localFS.close();
        for (File tmpDir : tmpDirs) {
            findTestUtil.deleteDir(tmpDir);
        }
    }

    @Test(groups = { "unit" })
    public void testFindFilesOnly() throws Exception {
        File tmp = createTmpNameSpace();
        FSFindTestUtil.FSFindTestDataFile testData = findTestUtil.createTestBed("find.txt",
                tmp.getCanonicalPath(), false);
        FSFind fsFind = new FSFindImpl(localFS);
        verifyFindResults(fsFind, FSFindQuery.make(testData.getPathOrPattern()), testData,
                Integer.MAX_VALUE, testData.getFilter());
    }

    @Test(groups = { "unit" })
    public void testFindIncludeDirs() throws Exception {
        File tmp = createTmpNameSpace();
        FSFindTestUtil.FSFindTestDataFile testData = findTestUtil.createTestBed("findIncludeDirs" +
                ".txt",
                tmp.getCanonicalPath(), true);
        FSFind fsFind = new FSFindImpl(true, localFS);
        verifyFindResults(fsFind, FSFindQuery.make(testData.getPathOrPattern()), testData,
                Integer.MAX_VALUE, testData.getFilter());
    }

    @Test(groups = { "unit" })
    public void testPathFilter() throws Exception {
        File tmp = createTmpNameSpace();
        FSFindTestUtil.FSFindTestDataFile testData = findTestUtil.createTestBed("findfilter.txt",
                tmp.getCanonicalPath(), true);
        FSFind fsFind = new FSFindImpl(true, localFS);
        verifyFindResults(fsFind, FSFindQuery.make(testData.getPathOrPattern()), testData,
                Integer.MAX_VALUE, testData.getFilter());
    }

    @Test(groups = { "unit" })
    public void testBatching() throws Exception {
        File tmp = createTmpNameSpace();
        FSFindTestUtil.FSFindTestDataFile testData = findTestUtil.createTestBed("findbiglist.txt",
                tmp.getCanonicalPath(), false);
        FSFind fsFind = new FSFindImpl(localFS);
        for (int i = 0; i < 5; i++) {
            FSFindResult batch = fsFind.find(FSFindQuery.make(testData.getPathOrPattern()),
                    testData.getPurgeTime(), 2);
            Assert.assertEquals(batch.candidates().size(), 2);
            deletePaths(batch.candidates()); // delete paths before resuming search
        }
        List<Path> shouldBeEmpty = fsFind.find(FSFindQuery.make(testData.getPathOrPattern()),
                testData.getPurgeTime());
        Assert.assertTrue(shouldBeEmpty.size() == 0,
                String.format("All paths should have been deleted, but found %d left.",
                        shouldBeEmpty.size()));
    }

    @Test(groups = { "unit" })
    public void testBatchingResumeFromOptimization() throws Exception {
        File tmp = createTmpNameSpace();
        FSFindTestUtil.FSFindTestDataFile testData = findTestUtil.createTestBed("findresumesearch" +
                ".txt",
                tmp.getCanonicalPath(), true);
        FSFind fsFind = new FSFindImpl(true, localFS); //include directories

        /* First batch, we expect certain path(s) to be fully explored */
        FSFindResult result1 = fsFind.find(FSFindQuery.make(testData.getPathOrPattern()),
                testData.getPurgeTime(), 5, testData.getFilter());
        /*
         * Since ls output is platform dependent we can not deteministically assert the batch
         * contents. However the way our test data is setup, atleast one directory should be fully
         * explored by end of batch1/
         */
        Assert.assertTrue(result1.explored().size() >= 1, "Should've finished exploring atleast " +
                "one directory completely.");
        Assert.assertEquals(result1.size(), 5);

        /* Second batch, we expect already explored paths to not be 'reexplored' */
        FSFindResult result2 = fsFind.find(FSFindQuery.makeFromResult(testData.getPathOrPattern(),
                result1), testData.getPurgeTime(), 10, testData.getFilter());
        Set shouldNotBeEmpty = Sets.difference(Sets.newHashSet(result1.candidates()),
                Sets.newHashSet(result2.candidates()));
        Assert.assertFalse(shouldNotBeEmpty.isEmpty());
        for (Path candidate : result2.candidates()) {
            // The files returned in second batch should not have come from any of the directories
            // that were marked as explored in first batch.
            Assert.assertFalse(result1.explored().contains(candidate.getParent()));
        }

        /* delete both batches */
        deletePaths(result1.candidates());
        deletePaths(result2.candidates());

        /* we should be left with 4 paths */
        List<File> filesPostDelete = Lists.newArrayList();
        findTestUtil.allFiles(new File(testData.getPathOrPattern().toUri().getPath()),
                filesPostDelete);
        Assert.assertEquals(filesPostDelete.size(), 4);

    }

    @Test(groups = { "unit" })
    public void testBatchingWithoutOptimization() throws Exception {
        File tmp = createTmpNameSpace();
        FSFindTestUtil.FSFindTestDataFile testData = findTestUtil.createTestBed("findresumesearch" +
                ".txt",
                tmp.getCanonicalPath(), true);
        FSFind fsFind = new FSFindImpl(true, localFS); //include directories
        FSFindResult result1 = fsFind.find(FSFindQuery.make(testData.getPathOrPattern()),
                testData.getPurgeTime(), 5, testData.getFilter());
        FSFindResult result2 = fsFind.find(FSFindQuery.make(testData.getPathOrPattern()),
                testData.getPurgeTime(), 5, testData.getFilter());
        Set shouldBeEmpty = Sets.difference(Sets.newHashSet(result1.candidates()),
                Sets.newHashSet(result2.candidates()));
        Assert.assertTrue(shouldBeEmpty.isEmpty());
        deletePaths(result2.candidates());

        FSFindResult result3 = fsFind.find(FSFindQuery.make(testData.getPathOrPattern()),
                testData.getPurgeTime(), 5, testData.getFilter());
        deletePaths(result3.candidates());
        FSFindResult result4 = fsFind.find(FSFindQuery.make(testData.getPathOrPattern()),
                testData.getPurgeTime(), 5, testData.getFilter());
        Assert.assertEquals(result4.size(), 0);

        /* we should be left with 4 paths */
        List<File> filesPostDelete = Lists.newArrayList();
        findTestUtil.allFiles(new File(testData.getPathOrPattern().toUri().getPath()),
                filesPostDelete);
        Assert.assertEquals(filesPostDelete.size(), 4);

    }

    @Test(groups = { "unit" })
    public void testFileNotFoundException() throws Exception {
        File tmp = createTmpNameSpace();
        FSFindTestUtil.FSFindTestDataFile testData = findTestUtil.createTestBed("findexception" +
                ".txt", tmp.getCanonicalPath(), true);

        Path subdir = new Path(LocalFileSystem.DEFAULT_FS + tmp.getCanonicalPath() +
                "/user/johndoe/subdir");
        FileSystem fs = FileSystem.getLocal(new Configuration());
        FileSystem spy = Mockito.spy(fs);
        Mockito.when(spy.getFileStatus(subdir)).thenThrow(new FileNotFoundException());
        FSFind fsFind = new FSFindImpl(true, spy); //include directories
        FSFindResult result = fsFind.find(FSFindQuery.make(testData.getPathOrPattern()),
                testData.getPurgeTime(), 5, testData.getFilter());
        Assert.assertEquals(result.size(), 3);
        fs.close();
    }

    @Test(groups = { "unit" }, expectedExceptions = IllegalStateException.class)
    public void testInvalidSearch() throws IOException {
        FSFind fsFind = new FSFindImpl(true, localFS);
        fsFind.find(FSFindQuery.make(new Path("/this_cant_exist")), Long.MAX_VALUE);
    }

    /**
     * Verify count, value of included items & return the actual result for further assertions.
     */
    private FSFindResult verifyFindResults(FSFind fsFind, FSFindQuery query,
            FSFindTestUtil.FSFindTestDataFile testData,
            int batch, PathFilter filter) throws IOException {
        FSFindResult actual = fsFind.find(query, testData.getPurgeTime(), batch, filter);
        boolean usingBatching = (batch != Integer.MAX_VALUE);

        /* verify candidate list size */
        if (usingBatching) {
            //it should never be more than the batch
            //the real size is hard to guess!
            Assert.assertTrue(actual.size() <= batch, String.format("Returned items are more than" +
                    " batch, " +
                    "batch = %s ", actual));
        } else {
            Assert.assertEquals(actual.size(), testData.getIncludeTotalCount());
        }
        /* verify the content of the list */
        for (FSFindTestUtil.FSFindTestDataEntry entry : testData.getEntries()) {
            if (entry.isInclude()) {
                if (!usingBatching) { // hard to verify what comes back when using batching
                    Assert.assertTrue(actual.candidates().contains(entry.getPath()),
                            String.format("%s should have been included in returned list.",
                                    entry.getPath()));
                }
            } else {
                Assert.assertFalse(actual.candidates().contains(entry.getPath()),
                        String.format("%s should not have been included in returned list.",
                                entry.getPath()));
            }
        }
        return actual;
    }

    private File createTmpNameSpace() {
        File tmp = Files.createTempDir();
        tmpDirs.add(tmp);
        return tmp;
    }

    private void deletePaths(List<Path> paths) throws IOException {
        for (Path path : paths) {
            localFS.delete(path, true);
        }
    }

}
