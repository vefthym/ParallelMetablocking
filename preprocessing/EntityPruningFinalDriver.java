/* 
 * Copyright (C) 2015 Vasilis Efthymiou <vefthym@ics.forth.gr>
 */
package preprocessing;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.SequenceFileInputFormat;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.lib.IdentityMapper;


public class EntityPruningFinalDriver {	
	

	public static void main(String[] args) {
		JobClient client = new JobClient();
		JobConf conf = new JobConf(preprocessing.EntityPruningFinalDriver.class);

		conf.setJobName("Entity Pruning Final Step");
		
		conf.setOutputKeyClass(VIntWritable.class);	 //block id
		conf.setOutputValueClass(VIntArrayWritable.class); //list of VIntWritables (entity ids of this block)
		
//		conf.setMapOutputKeyClass(VIntWritable.class);
//		conf.setMapOutputValueClass(VIntWritable.class);		
//				
		conf.setInputFormat(SequenceFileInputFormat.class);
		conf.setOutputFormat(SequenceFileOutputFormat.class);		
		SequenceFileOutputFormat.setOutputCompressionType(conf,	CompressionType.BLOCK);		
		
	//	conf.setCompressMapOutput(true);
		
		FileInputFormat.setInputPaths(conf, new Path(args[0])); //input blocking collection (initial input)
		FileOutputFormat.setOutputPath(conf, new Path(args[1])); //blocking collection without singulars and unary blocks

		conf.setMapperClass(preprocessing.EntityPruningFinalMapper.class);
//		conf.setMapperClass(IdentityMapper.class);		
//		conf.setReducerClass(preprocessing.EntityPruningFinalReducer.class); 	
		
		//conf.set("mapred.reduce.slowstart.completed.maps", "1.00");
		
		conf.setNumReduceTasks(0);
		
		Set<String> nonSingulars = new HashSet<>();		
		try{
			System.out.println("Retrieving nonSingular entities...");
			Path pt=new Path("/user/hduser/nonSingulars.txt");
            FileSystem fs = FileSystem.get(new Configuration());
            BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(pt)));
            String line;
            int lineCounter = 0;
            while ((line = br.readLine()) != null) {				
				nonSingulars.add(line);
				if ((++lineCounter % 10000) == 0) {
					System.out.println(lineCounter/1000+"K lines read...");
				}
			}           
            br.close();
            System.out.println(nonSingulars.size()+" nonSingular entities successfully retrieved.");
            System.out.println("Creating nonSingulars file...");
            String uniquePath = "/user/hduser/nonSingularsUnique.txt";
            Path pt2=new Path(uniquePath);
            BufferedWriter br2=new BufferedWriter(new OutputStreamWriter(fs.create(pt2,true)));        
            for (String nonSingular : nonSingulars) {
            	br2.write(nonSingular);
            	br2.newLine();
            }
            br2.close();            
            DistributedCache.addCacheFile(new URI(uniquePath), conf);
		}catch (URISyntaxException e1) {
    		System.err.println(e1.toString());    	
	    }catch(Exception e){
	    	System.err.println(e.toString());
	    }
		conf.set("mapred.user.jobconf.limit", "10485760");		
		
//		try {			
//			DistributedCache.addCacheFile(new URI("/user/hduser/nonSingulars.txt"), conf); // nonSingular entities (with duplicate entries)
//		} catch (URISyntaxException e1) {
//			System.err.println(e1.toString());
//		}
				
		client.setConf(conf);
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
