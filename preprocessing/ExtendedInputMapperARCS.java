package preprocessing;

import java.io.IOException;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import preprocessing.VIntArrayWritable;



public class ExtendedInputMapperARCS extends MapReduceBase implements Mapper<VIntWritable, VIntArrayWritable, VIntWritable, VIntArrayWritable> {

	VIntArrayWritable toEmit = new VIntArrayWritable();
	
	/**
	 * maps an input entity index into (key, value) pair(s)
	 * the value is the entity id (input key) along with the ids of blocks that contain it
	 * the key each time is a block id (each element of the input value array)
	 * @param key an entity id
	 * @param value an array of [blockId,blockSize] of the blocks that this entity belongs to
	 * @param output key: a block id (each element of the input value array) - value: the entity id (input key), the ids of blocks containing this entity (Bi)  
	 */
	public void map(VIntWritable key, VIntArrayWritable value,
			OutputCollector<VIntWritable, VIntArrayWritable> output, Reporter reporter) throws IOException {
		 
		VIntWritable [] Bi = value.get(); 	
		VIntWritable[] iWithBi = new VIntWritable[Bi.length+1];
		iWithBi[0] = key; //the first element is the entity i
		reporter.progress();
		System.arraycopy(Bi, 0, iWithBi, 1, Bi.length); //the remaining elements are the blocks of i (Bi)
		reporter.progress();
		toEmit.set(iWithBi);		

		//VIntWritable[] uncompressed = hadoopUtils.RelativePositionCompression.uncompress(value).get();
		for (int i = 0; i < Bi.length; i+=2) { //i+=2 to skip blockSizes
			VIntWritable bi = Bi[i];
			output.collect(bi, toEmit);			
		}
		
	}

}
