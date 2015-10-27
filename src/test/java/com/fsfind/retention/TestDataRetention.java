package com.fsfind.retention;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

import com.fsfind.FSFindTestUtil;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestDataRetention {

    private final CommandLineParser parser = new GnuParser();
    private FileSystem localFS;
    private List<File> tmpDirs = new ArrayList<File>();
    private FSFindTestUtil findTestUtil = new FSFindTestUtil();
    private DataRetention retention;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss.SSS");
    private Options options;

    @BeforeClass(groups = {"unit"})
    public void setUp() throws Exception {
        Configuration conf = new Configuration();
        localFS = FileSystem.getLocal(conf);
        retention = new DataRetention(localFS);
        retention.toggleDryRun(false);
        DataRetention.silentLogger();
        options = retention.buildOptions();
    }

    @AfterClass(groups = {"unit"})
    public void tearDown() throws Exception {
        localFS.close();
        for (File tmpDir : tmpDirs) {
            findTestUtil.deleteDir(tmpDir);
        }
    }

    /*
     * Simple test, create recent files and one day old files, run retention with one day as
     * purge time and verify the recent files are still there
     */
    @Test(groups = {"unit"})
    public void testSimpleRetention() throws Exception {
        File base = createTmpNameSpace();

        long now = System.currentTimeMillis();

        /* create recent files & directories */
        File dirA = new File(base, dateFormat.format(new Date(now)));
        List<File> fileUnderDirA = touchFiles(now, dirA);
        touchFile(now, dirA, true);

        /* create old files & directories */
        long oneDayAgo = nTimeUnitsAgo(now, 1, TimeUnit.DAYS);
        File dirB = new File(base, dateFormat.format(new Date(oneDayAgo)));
        touchFiles(oneDayAgo, dirB);
        touchFile(oneDayAgo, dirB, true);

        applyRetention(1, base.getCanonicalPath());

        /* assert remaining contents are as expected (all files under dir A) */
        List<File> actual = Lists.newArrayList();
        findTestUtil.allFiles(base, actual);
        List<File> expected = Lists.newArrayList(fileUnderDirA);
        expected.add(dirA);
        Assert.assertEqualsNoOrder(actual.toArray(), expected.toArray());

    }

    /*
     * Create files within same day but different hours, run retention with one day as purge time
     * and verify no file gets deleted
     */
    @Test(groups = {"unit"})
    public void testRetentionSameDayFiles() throws Exception {
        File base = createTmpNameSpace();

        long now = System.currentTimeMillis();

        /* create recent files & directories */
        File dirA = new File(base, dateFormat.format(new Date(now)));
        List<File> fileUnderDirA = touchFiles(now, dirA);
        touchFile(now, dirA, true);

        /* create files one hour ago */
        long oneHourAgo = nTimeUnitsAgo(now, 1, TimeUnit.HOURS);
        File dirB = new File(base, dateFormat.format(new Date(oneHourAgo)));
        List<File> fileUnderDirB = touchFiles(oneHourAgo, dirB);
        touchFile(oneHourAgo, dirB, true);

        applyRetention(1, base.getCanonicalPath());

        /* assert remaining contents are as expected (all files under dir A) */
        List<File> actual = Lists.newArrayList();
        findTestUtil.allFiles(base, actual);
        List<File> expected = Lists.newArrayList(fileUnderDirA);
        expected.addAll(fileUnderDirB);
        expected.add(dirA);
        expected.add(dirB);
        Assert.assertEqualsNoOrder(actual.toArray(), expected.toArray());
    }

    /*
     * Even if all files under the base (path where search begins) path are older than our purge
     * time, we should not delete the base path
     */
    @Test(groups = {"unit"})
    public void testBasePathNeverGetsDeleted() throws IOException {
        File base = createTmpNameSpace();
        long oneDayAgo = nTimeUnitsAgo(System.currentTimeMillis(), 1, TimeUnit.DAYS);
        touchFiles(oneDayAgo, base);
        int deleteCount = applyRetention(1, base.getCanonicalPath());
        Assert.assertTrue(base.exists());
        Assert.assertEquals(base.listFiles().length, 0);
        Assert.assertEquals(deleteCount, 5);
    }

    /* If all files under a sub directory are old, delete the sub directory */
    @Test(groups = {"unit"})
    public void testWholeSubDirGetsDeleted() throws IOException {
        File base = createTmpNameSpace();

        long oneDayAgo = nTimeUnitsAgo(System.currentTimeMillis(), 1, TimeUnit.DAYS);
        File subDir = new File(base, dateFormat.format(oneDayAgo));
        touchFiles(oneDayAgo, subDir);
        touchFile(oneDayAgo, subDir, true);

        int deleteCount = applyRetention(1, base.getCanonicalPath());
        Assert.assertTrue(base.exists());
        Assert.assertFalse(subDir.exists());
        Assert.assertEquals(base.listFiles().length, 0);
        Assert.assertEquals(deleteCount, 1);
    }

    /* If subdir is empty, delete it completely - only if its old */
    @Test(groups = {"unit"})
    public void testEmptySubDirs() throws IOException {
        File base = createTmpNameSpace();

        long oneDayAgo = nTimeUnitsAgo(System.currentTimeMillis(), 1, TimeUnit.DAYS);
        long twoDayAgo = nTimeUnitsAgo(System.currentTimeMillis(), 2, TimeUnit.DAYS);

        File sub1 = new File(base, "sub1");
        File sub2 = new File(base, "sub2");
        File sub3 = new File(sub2, "sub3"); // sub3 is inside sub2

        touchFile(oneDayAgo, sub1, true);
        touchFile(twoDayAgo, sub3, true);
        touchFile(twoDayAgo, sub2, true); // touch 2 after since 2 is top level

        // delete files older than 2 days
        int deleteCount = applyRetention(2, base.getCanonicalPath());

        // although sub1 is empty but its not old, so wont delete
        Assert.assertTrue(sub1.exists());
        Assert.assertFalse(sub2.exists());
        Assert.assertEquals(deleteCount, 1);

        // rerun retention with tighter policy & this time sub1 should be gone
        applyRetention(1, base.getCanonicalPath());
        Assert.assertFalse(sub1.exists());

        // base never gets deleted
        Assert.assertTrue(base.exists());
    }

    @Test(groups = {"unit"})
    public void tesMatchingDirectories() throws Exception {
        File base = createTmpNameSpace();
        File sub1 = new File(base, "sub1");
        File sub2 = new File(base, "sub2");
        File sub3 = new File(sub2, "sub3"); // sub3 is inside sub2
        sub1.mkdir();
        sub2.mkdir();
        sub3.mkdir();
        String pathPattern = base.getCanonicalPath() + "/sub*";
        List<Path> dirs = retention.matchingDirectories(pathPattern);
        Assert.assertEquals(dirs.size(), 2);
        Assert.assertEqualsNoOrder(dirs.toArray(), new Path[]{fileToPath(sub1), fileToPath(sub2)});
    }

    @Test(groups = {"unit"})
    public void testDeepDirectoryStructures() throws Exception {
        /* test case's directory lay out */
        // |_base/sub1
        //   base/sub1/file1 (recent)
        //   base/sub1/file2 (old)
        //   base/sub1/sub2
        //   base/sub1/sub2/file1 (recent)
        //   base/sub1/sub2/file2 (old)
        //   base/sub1/sub2/sub3
        //   base/sub1/sub2/sub3/file1 (old)
        //   base/sub1/sub2/sub3/file2 (old)
        //   base/sub1/sub2/sub3/sub4 (empty but not old)

        File base = createTmpNameSpace();
        long now = System.currentTimeMillis();
        long oneDayAgo = nTimeUnitsAgo(now, 1, TimeUnit.DAYS);

        File sub1 = new File(base, "sub1");
        File sub2 = new File(sub1, "sub2"); // sub2 is inside sub1
        File sub3 = new File(sub2, "sub3"); // sub3 is inside sub2
        File sub4 = new File(sub3, "sub4"); // sub4 is inside sub3

        List<File> survivors = Lists.newArrayList();
        survivors.addAll(Lists.newArrayList(sub1, sub2, sub3, sub4));

        /* create mix of recent and old files */
        touchFiles(oneDayAgo, sub1);
        touchFiles(oneDayAgo, sub2);
        touchFiles(oneDayAgo, sub3);
        survivors.addAll(touchFiles(now, sub1));
        survivors.addAll(touchFiles(now, sub2));
        survivors.addAll(touchFiles(now, sub3));

        /* update directory timestamps */
        touchFile(now, sub4, true);
        touchFile(oneDayAgo, sub3, true);
        touchFile(oneDayAgo, sub2, true);
        touchFile(oneDayAgo, sub1, true);

        applyRetention(1, base.getCanonicalPath());
        List<File> actualSurvivors = Lists.newArrayList();
        findTestUtil.allFiles(base, actualSurvivors);
        Assert.assertEqualsNoOrder(actualSurvivors.toArray(), survivors.toArray());
    }

    @Test(groups = {"unit"})
    public void testMultiplePathsUnderOnePolicy() throws Exception {
        File baseA = createTmpNameSpace();
        File baseB = createTmpNameSpace();

        long now = System.currentTimeMillis();
        long oneDayAgo = nTimeUnitsAgo(now, 1, TimeUnit.DAYS);
        File subA = new File(baseA, "subA");
        File subB = new File(baseB, "subB");

        List<File> expected = touchFiles(now, subA);
        touchFiles(oneDayAgo, subA);
        touchFiles(oneDayAgo, subB);

        applyRetention(1, baseA.getCanonicalPath(), baseB.getCanonicalPath());
        List<File> actual = Lists.newArrayList();
        findTestUtil.allFiles(baseA, actual);
        findTestUtil.allFiles(baseB, actual);

        expected.add(subA);
        Assert.assertEqualsNoOrder(actual.toArray(), expected.toArray());
        Assert.assertTrue(baseA.exists());
        Assert.assertTrue(baseB.exists());
    }

    @Test(groups = {"unit"})
    /* Non existing dir don't throw exception */
    public void testRetentionNonExistingPath() throws Exception {
        int deleteCount = applyRetention(1, "/nonExisting");
        Assert.assertEquals(deleteCount, 0);
        deleteCount = applyRetention(1, "/nonExisting/*"); //glob pattern
        Assert.assertEquals(deleteCount, 0);
    }

    @Test(groups = {"unit"})
    public void testIsValidOption() throws Exception {
        String mockConfFile = "test.xml";
        String mockPolicy = "testPolicy";
        String mockHdfsPath = "/use/warehouse";
        String mockNumDays = "30";

        // Test give both CONF_FILE and NUM_DAYS. Expect false.
        String[] args = new String[]{
                "-" + DataRetention.CONF_FILE, mockConfFile, "-" + DataRetention.NUM_DAYS, mockNumDays,
        };
        CommandLine cl = parser.parse(options, args);
        Assert.assertFalse(retention.isValidOption(cl), "");

        // Test give neither CONF_FILE nor HDFS_PATH. Expect false.
        args = new String[]{
                "-" + DataRetention.POLICY, mockPolicy, "-" + DataRetention.NUM_DAYS, mockNumDays,
        };
        cl = parser.parse(options, args);
        Assert.assertFalse(retention.isValidOption(cl));

        // Test give HDFS_PATH but not NUM_DAYS. Expect false.
        args = new String[]{
                "-" + DataRetention.HDFS_PATH, mockHdfsPath,
        };
        cl = parser.parse(options, args);
        Assert.assertFalse(retention.isValidOption(cl));

        // Test give CONF_FILE and POLICY. Expect true.
        args = new String[]{
                "-" + DataRetention.CONF_FILE, mockConfFile, "-" + DataRetention.POLICY, mockPolicy,
        };
        cl = parser.parse(options, args);
        Assert.assertTrue(retention.isValidOption(cl));

        // Test give HDFS_PATH and NUM_DAYS. Expect true.
        args = new String[]{
                "-" + DataRetention.HDFS_PATH, mockHdfsPath, "-" + DataRetention.NUM_DAYS, mockNumDays,
        };
        cl = parser.parse(options, args);
        Assert.assertTrue(retention.isValidOption(cl));

        // Test give only CONF_FILE. Expect true.
        args = new String[]{
                "-" + DataRetention.CONF_FILE, mockConfFile,
        };
        cl = parser.parse(options, args);
        Assert.assertTrue(retention.isValidOption(cl));
    }

    private Path fileToPath(File file) throws IOException {
        return new Path(LocalFileSystem.DEFAULT_FS + file.getCanonicalPath());
    }

    private int applyRetention(int days, String... paths) throws IOException {
        Map<String, Integer> pathMappings = Maps.newHashMap();
        for (String path : paths) {
            pathMappings.put(path, days);
        }
        DataRetentionPolicy policy = new DataRetentionPolicy(Integer.MAX_VALUE, pathMappings);
        return retention.applyPolicy(policy);
    }

    private void touchFile(long time, File fileToCreate, boolean isDirectory) throws IOException {
        if (isDirectory) {
            fileToCreate.mkdirs();
        } else {
            fileToCreate.createNewFile();
        }
        fileToCreate.setLastModified(time);
    }

    private List<File> touchFiles(long time, File parent) throws IOException {
        parent.mkdirs();
        List<File> files = Lists.newArrayList();
        for (int i = 0; i < 5; i++) {
            File fileToCreate = new File(parent, String.format("%s_%d.txt",
                    dateFormat.format(new Date(time)), i));
            touchFile(time, fileToCreate, false);
            files.add(fileToCreate);
        }
        return files;
    }

    private long nTimeUnitsAgo(long time, int n, TimeUnit unit) {
        return time - unit.toMillis(n);
    }

    private File createTmpNameSpace() {
        File tmp = Files.createTempDir();
        tmpDirs.add(tmp);
        return tmp;
    }

}
