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

* Read the data from the datanodes in parallel
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

Take the result jar file `target/ECValidator-1.0-SNAPSHOT.jar` and copy to a node on the Hadoop cluster.

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
