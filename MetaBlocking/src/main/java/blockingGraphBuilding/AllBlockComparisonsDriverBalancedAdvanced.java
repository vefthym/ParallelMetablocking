/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package blockingGraphBuilding;

import hadoopUtils.Partition;
import hadoopUtils.PartitionComparator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.TreeMap;

import org.apache.commons.collections.comparators.ReverseComparator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;


import blockingGraphBuilding.AllBlockComparisonsReducer.OutputData;


public class AllBlockComparisonsDriverBalancedAdvanced extends Configured {

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(blockingGraphBuilding.AllBlockComparisonsDriverBalancedAdvanced.class);
		
		conf.setJobName("AllBlockComparisons Balanced (Dirty)");
		
		conf.setMapOutputKeyClass(VIntWritable.class);
		conf.setMapOutputValueClass(Text.class);
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(VIntWritable.class);
		
		conf.setInputFormat(SequenceFileInputFormat.class);
		//conf.setOutputFormat(TextOutputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);

		FileInputFormat.setInputPaths(conf, new Path(args[0])); //input path in HDFS (Entity Index)
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //output path in HDFS (blocks)

		conf.setMapperClass(blockingGraphBuilding.AllBlockComparisonsMapper.class);
		//conf.setReducerClass(blockingGraphBuilding.AllBlockComparisonsReducer.class);
		conf.setReducerClass(blockingGraphBuilding.AllBlockComparisonsReducerDirty.class);
		
		
		
		
		//a block is a map entry with key: blockId, value: #comparisons
		Map<Integer,Long> blocks = new LinkedHashMap<>(); //keeps order of insertion (blocks are already sorted descending)
		
		try{
			Path pt=new Path("/user/hduser/afterFilteringBlockSizes.txt");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            
            String line;
            while ((line = br.readLine()) != null) {
            	String[] block = line.split("\t");
            	int blockId = Integer.parseInt(block[0]);
            	long blockComparisons = Long.parseLong(block[1]);
            	blockComparisons = (blockComparisons * (blockComparisons-1) ) / 2;  //dirty comparisons          	
            	blocks.put(blockId, blockComparisons);            	
            }            
            br.close();
	    }catch(Exception e){
	    	System.err.println(e.toString());
	    }
		
		
		//one partition for the largest block
        Map.Entry<Integer, Long> largestBlock = blocks.entrySet().iterator().next();
        blocks.remove(largestBlock.getKey());
        Partition seedPartition = new Partition();
        seedPartition.addBlock(largestBlock);
        
        //maximum comparisons per partition        
        final long partitionComparisons = largestBlock.getValue();
        System.out.println("Partition comparisons\t:\t" + partitionComparisons);
        		
		Queue<Partition> pq = new PriorityQueue<>(blocks.size(), new PartitionComparator());
		pq.add(seedPartition);
		
		//Map<Integer,Integer> blockPartitions = new HashMap<>(); //key: blockId, value:partition
		
		
		for (Map.Entry<Integer, Long> block : blocks.entrySet()) {
			Partition smallestPartition = pq.poll();
			long totalComparisons = smallestPartition.getTotalComparisons()+block.getValue();
			if (totalComparisons < partitionComparisons) { //if the new block fits into the smallest partition
				smallestPartition.addBlock(block); //add it to the partition
			} else { //otherwise create a new partition for the current block
				Partition newPartition = new Partition();
				newPartition.addBlock(block);
				pq.add(newPartition);
			}
			pq.add(smallestPartition);
		}
		
		int noOfPartitions = pq.size();
		System.out.println("Total partitions\t:\t" + noOfPartitions);
		
		

		try{            
            Path pt2=new Path("/user/hduser/blockPartitions.txt");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedWriter bw=new BufferedWriter(new OutputStreamWriter(fs.create(pt2,true)));            		
            
            
            
            //store partitions from largest to smallest
            for (int i = noOfPartitions-1; i >= 0; --i) {
            	Partition partition = pq.poll();
            	
            	String paritionId = Integer.toString(i);
            	
            	for (Integer blockId : partition.getBlockIds()) { //write the mapping to a file, that will later be added to the DistributedCache
            		 bw.write(Integer.toString(blockId));
                     bw.write("\t");
                     bw.write(paritionId);
                     bw.newLine();
            	}
            	
            }
            bw.close();
            DistributedCache.addCacheFile(new URI(pt2.toString()), conf);
		} catch(Exception e){
	    	System.err.println(e.toString());
	    }
            
		
		
		conf.setNumReduceTasks(noOfPartitions);		
		conf.setPartitionerClass(blockingGraphBuilding.AllBlockComparisonsParitioner.class);

		client.setConf(conf);
		RunningJob job = null;
		try {
			job = JobClient.runJob(conf);			
		} catch (Exception e) {
			e.printStackTrace();
		}	
			
		if (job == null) {
			System.err.println("No job found");
			return;
		}
	}
		
	

}
