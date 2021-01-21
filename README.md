# HDFS EC File Validator

Due to some Erasure Coding data corruption bugs in HDFS, it is useful to have a tool which can read specified Erasure Coded files and report if they appear to be valid or not.

One specific HDFS bug (HDFS-15186 and HDFS-14768) can result in a parity block getting overwritten with all Zero bytes. Then further EC Block recovery can occur causing the corruption to progress to data blocks.

For example, with a EC 6-3 file:

* The parity blocks are at indexes 6, 7 and 8.
* Parity 6 gets overwritten with zeros.
* Then data 0 is lost and regenerated using data 1 - 5 and parity 6.
* This will result in a corrupt data 0 block.

If we then use data blocks 0 - 5 to regenerate the parity blocks 6, 7 and 8:

* 6 will come out as all zero and appear correct.
* 7 and 8 will different from the original encodings, as the data in 0 has changed due to the corruption.

Provided less than "parity number" of blocks are reconstructed at the same time, it should be possible to detect a corruption of this kind.

Even if more than "parity number" of individual reconstructions occur over time the corruption should still be detectable, but in the worst case it may be required to generate new blocks all permutations of the blocks in the group. Ie for all combinations of taking 6 from 9 (for EC-6-3), use the taken 6 to generate the remaining 3.

HDFS stores up to 128MB of data in each data block of the block group, but it encodes the data in stripes of 1MB * "data number" (6 for EC 6-3). Due to the nature of this corruption, it is not required to read the full blocks - it should be sufficient to read only 1MB from each data block and then perform the EC calculations.

## TODO

* Currently, only the parity is generated from the data blocks. Allow for all combinations to be checked as mentioned above.
* Provide a map-reduce job to allow many files to be checked in parallel across the cluster.


## Usage

Edit the pom.xml to match your cluster version:

```
  <properties>
    <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    <!-- for upstream hadoop versions just use "3.3.0", for example -->
    <hadoop.version>3.0.0-cdh6.3.4</hadoop.version>
  </properties>
```

Compile the project - the tests will take a minute or two to run:

```
mvn package
```

Take the resulting jar file `target/ECValidator-1.0-SNAPSHOT.jar` and copy to a node on the Hadoop cluster.

Set the classpath:

```
export CLASSPATH=`hadoop classpath`:ECValidator-1.0-SNAPSHOT.jar
```

Kinit if the cluster is secure, and then run the tool:

```
/usr/java/jdk1.8.0_141-cloudera/bin/java com.sodonnell.ECFileValidator /ecfiles/test1

21/01/11 21:10:41 WARN util.NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
21/01/11 21:10:42 INFO sodonnell.ECFileValidator: Going to validate /ecfiles/test1
21/01/11 21:10:42 INFO sodonnell.ECFileValidator: checking block BP-191582629-172.27.74.136-1610382327645:blk_-9223372036854775792_2887 of size 50331648
21/01/11 21:10:42 INFO sodonnell.ECFileValidator: checking block BP-191582629-172.27.74.136-1610382327645:blk_-9223372036854775776_2888 of size 50331648
21/01/11 21:10:42 INFO sodonnell.ECFileValidator: checking block BP-191582629-172.27.74.136-1610382327645:blk_-9223372036854775760_2889 of size 50331648
...
21/01/11 21:10:43 INFO sodonnell.ECFileValidator: checking block BP-191582629-172.27.74.136-1610382327645:blk_-9223372036854775456_2908 of size 16777216
The file is valid
```

The general usage is:

```
/usr/java/jdk1.8.0_141-cloudera/bin/java com.sodonnell.ECFileValidator <space seperated list of files>
```

There is also a batch mode, which takes a file as input and writes the results to stdout or an output file:

```
/usr/java/jdk1.8.0_141-cloudera/bin/java com.sodonnell.cli.BatchFile inputFile <optional_output_file>
```

The output looks like the following:

```
<healthy|corrupt|failed> <file path> <corrupt block groups or any error message>
Eg:
healthy /ecfiles/test1
corrupt /ecfiles/test2 blk_-9223372036854775440_5746
failed /ecfiles/test5 File /ecfiles/test5 does not exist
healthy /ecfiles/test3
failed /ecfiles/test4 Data block in position 0 of block BP-191582629-172.27.74.136-1610382327645:blk_-9223372036854774736_5791 is unavailable
```

## Map Reduce

To check many files, you can use a map reduce job. This job will partition all files under the given directories into a number of input files, searching the directories recursively. The job will start a mapper per generated input file and check the files in parallel.

The usage instructions are:

```
hadoop jar ECValidator-1.0-SNAPSHOT.jar com.sodonnell.mapred.ValidateFiles stagingDir outputDir numSplits <one or more directories to check>

eg

hadoop jar ECValidator-1.0-SNAPSHOT.jar com.sodonnell.mapred.ValidateFiles ecstage ecoutput 5 /ecfiles
```

The output looks like the following, under the output directory:

```
# hadoop fs -cat ecoutput/part-r-00000
corrupt	/ecfiles/test2 blk_-9223372036854775440_5746
failed	/ecfiles/notecreally File /ecfiles/notecreally is not erasure coded
failed	/ecfiles/test4 Data block in position 0 of block BP-191582629-172.27.74.136-1610382327645:blk_-9223372036854774736_5791 is unavailable
healthy	/ecfiles/copy3/copy2/notecreally
healthy	/ecfiles/copy3/test4
healthy	/ecfiles/copy3/test2
healthy	/ecfiles/copy3/notecreally
healthy	/ecfiles/copy3/copy2/test4
healthy	/ecfiles/copy3/copy2/test2
healthy	/ecfiles/copy3/copy2/copy1/test4
healthy	/ecfiles/copy3/copy1/test4
healthy	/ecfiles/copy2/test4
healthy	/ecfiles/copy2/test2
```
