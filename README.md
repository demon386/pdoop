pdoop

A golang written utilities to get files from a HDFS directory in parallel. In my working environment it's about 3.8 times faster than using `hadoop fs -getmerge` when merging as a single file.

It relies on $HADOOP_HOME environmental variable.

Usage:

`pdoop hdfs_dir local_path`

Options:

- `-m`: merge hdfs dir as a single file. It's disabled by default.
  NOTE: when merging as a single file, the order of different files are not guaranteed.
- `-p`: number of parallel downloads, 10 by default.
