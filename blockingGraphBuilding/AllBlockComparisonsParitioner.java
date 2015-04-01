package blockingGraphBuilding;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;

public class AllBlockComparisonsParitioner implements Partitioner<VIntWritable, Text>{

	//private final int MAX_BLOCK_ID = 1499534; //the number of lines in BlockSizes/part-00000 file

	Map<Integer, Integer> blockPartitions;
	Path[] localFiles;
	
	@Override
	public void configure(JobConf job) {
		blockPartitions = new HashMap<>();
		
		BufferedReader SW;
		try {
			localFiles = DistributedCache.getLocalCacheFiles(job); //for the cluster version			
			SW = new BufferedReader(new FileReader(localFiles[0].toString())); //for the cluster version
			String line;
			while ((line = SW.readLine()) != null) {
				if (line.trim().isEmpty()) {break;}
				String[] block = line.split("\t");
				blockPartitions.put(Integer.parseInt(block[0]), Integer.parseInt(block[1]));
			}
		    SW.close();
		} catch (FileNotFoundException e) {
			System.err.println(e.toString());
		} catch (IOException e) {
			System.err.println(e.toString());
		}
	}
	
	//option 4
	@Override
	public int getPartition(VIntWritable key, Text value, int numPartitions) {
		int blockId = key.get();
		return blockPartitions.get(blockId);
	}
	
//	//option 3
//	@Override
//	public int getPartition(VIntWritable key, Text value, int numPartitions) {
//		int blockId = key.get();
//		int inverseBlockId = MAX_BLOCK_ID - blockId; //the largest block is the one with MAX_BLOCK_ID
//		if (inverseBlockId / numPartitions == 0)  {// η πρώτη N-άδα
//			return inverseBlockId % numPartitions;
//		}else {
//			return numPartitions-1-inverseBlockId%numPartitions;
//		}
//	}
	
	//option 2
//	@Override
//	public int getPartition(VIntWritable key, Text value, int numPartitions) {
//		int blockId = key.get();
//		int inverseBlockId = MAX_BLOCK_ID - blockId; //the largest block is the one with MAX_BLOCK_ID
//		if (inverseBlockId / numPartitions % 2 == 0)  {// περιττή Ν-άδα
//			return inverseBlockId % numPartitions;
//		}else {
//			return numPartitions-1-inverseBlockId%numPartitions;
//		}
//	}
	
	//option 1
//	@Override
//	public int getPartition(VIntWritable key, Text value, int numPartitions) {
//		int blockId = key.get();
//		return (MAX_BLOCK_ID - blockId) % numPartitions;
//	}
	

	

	


}
