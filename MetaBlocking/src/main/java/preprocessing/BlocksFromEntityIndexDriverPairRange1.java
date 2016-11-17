/* 
 * Copyright (C) 2016 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package preprocessing;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;

import preprocessing.BlocksFromEntityIndexReducer.OutputData;





public class BlocksFromEntityIndexDriverPairRange1 extends Configured {

    public static void main(String[] args) {
        JobClient client = new JobClient();
        JobConf conf = new JobConf(preprocessing.BlocksFromEntityIndexDriverPairRange1.class);

        conf.setJobName("Blocks from Entity Index - PairRange1 (without numPartition restrictions)");

        conf.setMapOutputKeyClass(VIntWritable.class);
        conf.setMapOutputValueClass(VIntWritable.class);

        conf.setOutputKeyClass(VIntWritable.class); //block id
        conf.setOutputValueClass(VIntArrayWritable.class); //list of entities in this block

        conf.setInputFormat(SequenceFileInputFormat.class);
        conf.setOutputFormat(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);

        FileInputFormat.setInputPaths(conf, new Path(args[0])); //Entity Index (Filtered with block filtering)
        FileOutputFormat.setOutputPath(conf, new Path(args[1])); //Blocking Collection (Filtered with block filtering)

        conf.setMapperClass(preprocessing.BlocksFromEntityIndexMapper.class);
        conf.setReducerClass(preprocessing.BlocksFromEntityIndexReducer.class);		

        conf.setInt("mapred.task.timeout", 10000000);
//		conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
        conf.setMaxReduceTaskFailuresPercent(10);		
        conf.set("mapred.reduce.max.attempts", "10");
        conf.set("mapred.max.tracker.failures", "100");
        conf.set("mapred.job.tracker.handler.count", "40");

        conf.setCompressMapOutput(true);



        //////////////////////////////////
        //Here starts the balancing part//
        //////////////////////////////////


        //a block is a map entry with key: blockId, value: #comparisons
        Map<Integer,Long> blocks = new LinkedHashMap<>(); //keeps order of insertion (blocks are already sorted descending)

        long totalComparisons = 0;
        try{			
            FileSystem fs = FileSystem.get(new Configuration());
            Path pt=new Path("/user/hduser/afterFilteringBlockSizes.txt");            
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));

            String line;            
            while ((line = br.readLine()) != null) {
                String[] block = line.split("\t");
                int blockId = Integer.parseInt(block[0]);
                long blockComparisons = Long.parseLong(block[1]); //actually the size in bytes of the next mappers' output (already squared)
                blocks.put(blockId, blockComparisons); 
                
                totalComparisons += blockComparisons;
            }            
            br.close();

        }catch(Exception e){
            System.err.println(e.toString());
        }
        System.out.println("Total comparisons\t:\t" + totalComparisons);
        
        //parameters
        int numClusterNodes = 14; //default value
        final int numSlotsPerNode = 4;
        int numReduceRounds = 8; //default value 
        
        if (args.length == 4) {
                numClusterNodes = Integer.parseInt(args[2]);
                numReduceRounds = Integer.parseInt(args[3]);
        }

        final int SLOTS = numClusterNodes * numSlotsPerNode; //#nodes * #slots/node		
        final int NUM_PARTITIONS = numReduceRounds*SLOTS;
        
        //maximum comparisons per partition    
        final long PARTITION_COMPARISONS = totalComparisons / NUM_PARTITIONS;
        System.out.println("Partition comparisons\t:\t" + PARTITION_COMPARISONS);

        
        int partitionId = 0;
        try{            
            Path pt2=new Path("/user/hduser/blockPartitions.txt");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fs.create(pt2,true)));            		
        
            long sumComparisons = 0;
            int numBlocksInPartition = 0;
            while (!blocks.isEmpty()) {		        
                Map.Entry<Integer, Long> currentBlock = blocks.entrySet().iterator().next();
                blocks.remove(currentBlock.getKey());            
                sumComparisons += currentBlock.getValue();
                numBlocksInPartition++;
                
                bw.write(Integer.toString(currentBlock.getKey()));
                bw.write("\t");
                bw.write(Integer.toString(partitionId));
                bw.newLine();
                
                if (sumComparisons >= PARTITION_COMPARISONS) {
                    System.out.println("Partition "+partitionId+" contains "+numBlocksInPartition+" blocks with total comparisons "+sumComparisons);
                    partitionId++;
                    sumComparisons = 0;
                    numBlocksInPartition = 0;
                }
                                
            }
            System.out.println("Partition "+partitionId+" contains "+numBlocksInPartition+" blocks with total comparisons "+sumComparisons);
            
            bw.close();
            DistributedCache.addCacheFile(new URI(pt2.toString()), conf);

        } catch(Exception e){
            System.err.println(e.toString());
        }

        int actualPartitions = partitionId + 1;
        System.out.println("Total partitions\t:\t" + actualPartitions);
        

        conf.setNumReduceTasks(actualPartitions);		
        conf.setPartitionerClass(preprocessing.BlocksFromEntityIndexParitioner.class);


        client.setConf(conf);
        RunningJob job = null;		
        try {
            job = JobClient.runJob(conf);			
        } catch (Exception e) {
            e.printStackTrace();
        }

        //the following is used only for CNP,CEPTotalOrder but does not create any overhead (keep it always)		
        if (job == null) {
            System.err.println("No job found");
            return;
        }

        BufferedWriter bwClean = null;
        BufferedWriter bwDirty = null;
        try {								
            Counters counters = job.getCounters();
            Long dirtyBlocks = counters.findCounter("org.apache.hadoop.mapred.Task$Counter",
                            "REDUCE_OUTPUT_RECORDS").getCounter();
            Long cleanBlocks = counters.findCounter(OutputData.CLEAN_BLOCKS).getCounter();			
            Path cleanPath=new Path("/user/hduser/numBlocksClean.txt");
            Path dirtyPath=new Path("/user/hduser/numBlocksDirty.txt");
            FileSystem fs = FileSystem.get(new Configuration());
            bwClean = new BufferedWriter(new OutputStreamWriter(fs.create(cleanPath,true)));            
            bwDirty = new BufferedWriter(new OutputStreamWriter(fs.create(dirtyPath,true)));
            bwClean.write(cleanBlocks.toString());
            bwDirty.write(dirtyBlocks.toString());            
        } catch (IllegalArgumentException | IOException e) {			
            System.err.println(e.toString());
        } finally {
            try { bwClean.close(); bwDirty.close();	} 
            catch (IOException e) { System.err.println(e.toString());}			
        }
    }
		
}
