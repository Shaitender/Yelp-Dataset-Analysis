## Introduction
Pig is a platform for parallelized analysis of large data sets. Pig programs use a language called Pig Latin.
PIG scripts were executed on HUE Cluster with installed Apache Hadoop for MapReuce workflow.

### Required JARs for JSON loader, link these below mentioned jars to PIG editor:
- [elephant-bird-pig](https://mvnrepository.com/artifact/com.twitter.elephantbird/elephant-bird-pig)
- [elephant-bird-core](https://mvnrepository.com/artifact/com.twitter.elephantbird/elephant-bird-core)
- [elephant-bird-hadoop-compat](https://mvnrepository.com/artifact/com.twitter.elephantbird/elephant-bird-hadoop-compat)

## To run:
### HUE Cluster
- Upload the data to HUE HDFS
- Create the script and link jars to the editor
- Save and execute the script
- As jobs run, the status boxes will have all the logs, the output and error messages, if any.

### Grunt Shell
- You can also explore PIG shell to run the scripts
  - With local – Type `pig -x local` to enter the shell
  - With MapReduce – Type `pig -x mapreduce` to enter the shell
- Type `exec <filename.pig>` to run on respective shell
