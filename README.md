# ParallelMetablocking
Parallel Meta-blocking in MapReduce

Source code for parallelizing the Meta-Blocking techniques introduced in:

V. Efthymiou, G. Papadakis, G. Papastefanatos, K. Stefanidis, T. Palpanas: Parallel Meta-blocking: Realizing Scalable Entity Resolution over Large, Heterogeneous Data. IEEE Big Data Conference 2015.

The code is written in Java version 7, using Apache Hadoop, version 1.2.0. All experiments were performed on a cluster with 15 Ubuntu 12.04.3 LTS servers, one master and 14 slaves, each having 8 AMD 2.1 GHz CPUs and 8 GB of RAM. Each node can run 4 map or reduce tasks simultaneously, assigning 1024 MB to each task. The available disk space amounted to 4 TB and was equally partitioned among the 15 nodes.

The datasets are available on Google Drive on the following links:

    DB_C: https://drive.google.com/open?id=0B4aFCVb8nejsMkZsM21xTDBfaWc&authuser=0
    DB_D: https://drive.google.com/open?id=0B4aFCVb8nejsTWFTVmhnaFFmQnM&authuser=0
    FB_C: https://drive.google.com/open?id=0B4aFCVb8nejsUVNrc2NUUW15WWc&authuser=0
    FB_D: https://drive.google.com/open?id=0B4aFCVb8nejsem0tYWVtcS11TGc&authuser=0


* INSTRUCTION FOR EXECUTION (ADVANCED STRATEGY) *

You should run the main files for each of the following steps. To generate a hadoop jar, change the main class in the pom.ml file accordingly:

For Block Filtering:
Job 1: BlockSizeCounterDriver.java
Job 2: EntityIndexDriver.java

For preprocessing: 
ExtendedInputDriver.java

For WEP:
Job 1: AverageWeightDriver.java
Job 2: WEPDriver.java

For CEP:
Job 1: CEPCountingDriver.java
Job 2: CEPFinalDriver.java

For WNP:
WNPDriver.java

For CNP:
CNPDriver.java
