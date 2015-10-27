package com.sp;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class FSFindTestUtil {

    private static final String ONE = "1";
    private static final String DIR = ONE;
    private static final String INCLUDE = ONE;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMDDHHmm");

    /**
     * Parse the test data file. The prefix parameter is useful for creating paths in random tmp
     * spaces. Create a tmp directory inside /tmp with random suffic and pass '/tmp/<suffix>' as
     * path prefix to this method to have all test paths prefixed with your tmp name space.
     */
    public FSFindTestDataFile loadFile(String testFile, String pathPrefix,
            boolean dirMode) throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        URI url = classLoader.getResource(testFile).toURI();
        Scanner scanner = new Scanner(new File(url), "UTF8");
        Path pathOrPattern = null;
        long purgeTime = 0;
        PathFilter filter = null;

        List<FSFindTestDataEntry> entries = Lists.newArrayList();
        Set<Path> includedDirs = Sets.newHashSet();

        pathPrefix = LocalFileSystem.DEFAULT_FS + pathPrefix;

        while (scanner.hasNext()) {
            String currentLine = scanner.nextLine();
            String[] fields = currentLine.split(" ");
            if (!currentLine.startsWith("#")) {
                if (fields.length == 3) {
                    pathOrPattern = new Path(pathPrefix + "/" + fields[0]);
                    purgeTime = dateFormat.parse(fields[1]).getTime();
                    filter = FSFindFilters.valueOf(fields[2]);
                } else {
                    boolean include = fields[0].equals(INCLUDE);
                    boolean isDir = fields[1].equals(DIR);
                    Path path = new Path(pathPrefix + "/" + fields[2]);
                    if (isDir && include) {
                        includedDirs.add(path);
                    }
                    long mtime = dateFormat.parse(fields[3]).getTime();
                    entries.add(new FSFindTestDataEntry(include, isDir, path, mtime));
                }
            }
        }

        /* do a pass of entry set and remove files whose directories are already included */
        if (dirMode) {
            for (FSFindTestDataEntry entry : entries) {
                if (!entry.isDir && includedDirs.contains(entry.path.getParent())) {
                    entry.include = false;
                }
            }
        }

        return new FSFindTestDataFile(purgeTime, entries, filter, pathOrPattern);
    }

    public FSFindTestDataFile createTestBed(String testFile, String pathPrefix,
            boolean dirMode) throws Exception {
        return createTestBed(loadFile(testFile, pathPrefix, dirMode));
    }

    public FSFindTestDataFile createTestBed(FSFindTestDataFile testData) throws IOException {
        new File(testData.pathOrPattern.toUri().getPath()).mkdirs();
        for (FSFindTestDataEntry entry : testData.entries) {
            File fileEntry = new File(entry.path.toUri().getPath());
            if (entry.isDir) {
                fileEntry.mkdirs();
            } else {
                fileEntry.createNewFile();
            }
        }
        for (FSFindTestDataEntry entry : testData.entries) {
            File fileEntry = new File(entry.path.toUri().getPath());
            fileEntry.setLastModified(entry.mtime);
        }
        return testData;
    }

    public boolean deleteDir(File tmpFile) {
        if (tmpFile.exists()) {
            File[] files = tmpFile.listFiles();
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDir(file);
                } else {
                    file.delete();
                }
            }
        }
        return (tmpFile.delete());
    }

    public void allFiles(File dir, List<File> result) {
        File[] list = dir.listFiles();
        if (list == null) {
            return;
        }
        for (File f : list) {
            if (f.isDirectory()) {
                allFiles(f, result);
            }
            result.add(f);
        }
    }

    public class FSFindTestDataFile {
        private List<FSFindTestDataEntry> entries;
        private PathFilter filter;
        private long purgeTime;
        private Path pathOrPattern;
        private int includeTotalCount = 0;

        public FSFindTestDataFile(long purgeTime, List<FSFindTestDataEntry> entries,
                PathFilter filter, Path pathOrPattern) {
            this.purgeTime = purgeTime;
            this.entries = entries;
            this.filter = filter;
            this.pathOrPattern = pathOrPattern;
            initCounters();
        }

        private void initCounters() {
            for (FSFindTestDataEntry entry : entries) {
                if (entry.include) {
                    this.includeTotalCount++;
                }
            }
        }

        public List<FSFindTestDataEntry> getEntries() {
            return entries;
        }

        public void setEntries(List<FSFindTestDataEntry> entries) {
            this.entries = entries;
        }

        public PathFilter getFilter() {
            return filter;
        }

        public void setFilter(PathFilter filter) {
            this.filter = filter;
        }

        public long getPurgeTime() {
            return purgeTime;
        }

        public void setPurgeTime(long purgeTime) {
            this.purgeTime = purgeTime;
        }

        public Path getPathOrPattern() {
            return pathOrPattern;
        }

        public void setPathOrPattern(Path pathOrPattern) {
            this.pathOrPattern = pathOrPattern;
        }

        public int getIncludeTotalCount() {
            return includeTotalCount;
        }

        public void setIncludeTotalCount(int includeTotalCount) {
            this.includeTotalCount = includeTotalCount;
        }
    }

    public class FSFindTestDataEntry {
        private boolean include;
        private boolean isDir;
        private Path path;
        private long mtime;

        public FSFindTestDataEntry(boolean include, boolean isDir, Path path, long mtime) {
            this.include = include;
            this.isDir = isDir;
            this.path = path;
            this.mtime = mtime;
        }

        public boolean isInclude() {
            return include;
        }

        public void setInclude(boolean include) {
            this.include = include;
        }

        public boolean isDir() {
            return isDir;
        }

        public void setDir(boolean isDir) {
            this.isDir = isDir;
        }

        public Path getPath() {
            return path;
        }

        public void setPath(Path path) {
            this.path = path;
        }

        public long getMtime() {
            return mtime;
        }

        public void setMtime(long mtime) {
            this.mtime = mtime;
        }
    }

}
