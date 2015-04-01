# ParallelMetablocking
Parallel Meta-blocking in MapReduce 

Source code for parallelizing the Meta-Blocking techniques introduced in:

George Papadakis, Georgia Koutrika, Themis Palpanas, Wolfgang Nejdl: 
Meta-Blocking: Taking Entity Resolution to the Next Level. IEEE Trans. Knowl. Data Eng. 26(8): 1946-1960 (2014)

The code is written in Java version 7, using Apache Hadoop, version 1.2.0.
All experiments were performed on a cluster with 15 Ubuntu 12.04.3 LTS servers, 
one master and 14 slaves, each having 8 AMD 2.1 GHz CPUs and 8 GB of RAM. 
Each node can run 4 map or reduce tasks simultaneously, assigning 1024 MB to each task. 
The available disk space amounted to 4 TB and was equally partitioned among the 15 nodes.

The datasets are available on Google Drive on the following links:

1. DB_C: https://drive.google.com/open?id=0B4aFCVb8nejsMkZsM21xTDBfaWc&authuser=0
2. DB_D: https://drive.google.com/open?id=0B4aFCVb8nejsTWFTVmhnaFFmQnM&authuser=0
3. FB_C: https://drive.google.com/open?id=0B4aFCVb8nejsUVNrc2NUUW15WWc&authuser=0
4. FB_D: https://drive.google.com/open?id=0B4aFCVb8nejsem0tYWVtcS11TGc&authuser=0
