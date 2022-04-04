# ParallelMetablocking

Source code for parallelizing the Meta-Blocking techniques introduced in:

[V. Efthymiou, G. Papadakis, G. Papastefanatos, K. Stefanidis, T. Palpanas: Parallel Meta-blocking: Realizing Scalable Entity Resolution over Large, Heterogeneous Data. IEEE Big Data Conference 2015](https://doi.org/10.1109/BigData.2015.7363782)

and later extended with an entity-based strategy and MaxBlock load balancing algorithm in:

[V. Efthymiou, G. Papadakis, G. Papastefanatos, K. Stefanidis, T. Palpanas: Parallel Meta-blocking for Scaling Entity Resolution over Big Heterogeneous Data. Information Systems 65: 137-157 (2017)](https://doi.org/10.1016/j.is.2016.12.001)

**Datasets**: 

The datasets are available on Zenodo: https://zenodo.org/record/6408960#.YkmvwjVRVhE. <br/>
More datasets, including some of the ones also used in this work, are available on https://www.csd.uoc.gr/~vefthym/minoanER/datasets.html.


**Implementation Details:**

The code is written in Java version 7, using Apache Hadoop, version 1.2.0. All experiments were performed on a cluster with 15 Ubuntu 12.04.3 LTS servers, one master and 14 slaves, each having 8 AMD 2.1 GHz CPUs and 8 GB of RAM. Each node can run 4 map or reduce tasks simultaneously, assigning 1024 MB to each task. The available disk space amounted to 4 TB and was equally partitioned among the 15 nodes.

Entity-based CNP is also implemented in Apache Spark in a later work published in EDBT 2019:

V. Efthymiou, G. Papadakis, K. Stefanidis, V. Christophides. MinoanER: Schema-Agnostic, Non-Iterative, Massively Parallel Resolution of Web Entities. EDBT 2019.

The code of this work is also on github: https://github.com/vefthym/MinoanER and the folder containing the entity-based CNP implementation can be found here: https://github.com/vefthym/MinoanER/tree/master/src/main/java/minoaner/metablocking/entityBased

**INSTRUCTIONS FOR EXECUTION (ADVANCED STRATEGY)**:

You should run the main files for each of the following steps. To generate a hadoop jar, change the main class in the pom.ml file accordingly. 

For Block Filtering:<br/>
Job 1: BlockSizeCounterDriver.java <br/>
Job 2: EntityIndexDriver.java

For Preprocessing: <br/>
ExtendedInputDriver.java

For WEP:<br/>
Job 1: AverageWeightDriver.java<br/>
Job 2: WEPDriver.java

For CEP:<br/>
Job 1: CEPCountingDriver.java<br/>
Job 2: CEPFinalDriver.java

For WNP:<br/>
WNPDriver.java

For CNP:<br/>
CNPDriver.java


**INSTRUCTIONS FOR EXECUTION (ENTITY-BASED STRATEGY)**:

For Block Filtering:<br/>
Job 1: BlockSizeCounterDriver.java <br/>
Job 2: EntityIndexDriver.java <br/>
 
For Preprocessing:<br/>
BlocksFromEntityIndexDriver.java
 
For WNP:<br/>
EntityBasedDriverWNP.java

**DEPENDENCIES**:

The dependency to blockingFramework can be resolved by downloading the jar file from https://sourceforge.net/projects/erframework/<br/>
The source files there also contain very useful code for different blocking and meta-blocking techniques. 
