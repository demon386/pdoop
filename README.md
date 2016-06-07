pdoop

A golang written utilities to get files from a HDFS directory in parallel.

It relies on $HADOOP_HOME environmental variable.

Usage:

`pdoop hdfs_dir local_path`

Options:

- `-m`: merge hdfs dir as a single file. It's disabled by default.
- `-p`: number of parallel downloads, 10 by default.
