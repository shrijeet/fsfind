# fsfind

'fsfind' is a unix 'find' like tool for HDFS. Think: 

```
find . -mtime +10 -delete
```

This project provides both a CLI tool as well as an API for programmatic access to find files older than certain timestamp and act on those files based on configured policy.

Its highly optimized to work with HDFS nuiances and has been battle tested on a large HDFS setup.

Coming soon.. more details on the API and CLI specification.
