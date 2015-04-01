package preprocessing;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class EntityIdsToIntMapper extends MapReduceBase implements Mapper<Text, Text, VLongWritable, Text> {
	
	static enum InputData {MALFORMED_PAIRS, DBPEDIA, DATASOURCE2, WRAPAROUND};
	
	private int currentMapper;
	private int counter;
	public void configure(JobConf conf) {
        currentMapper = Integer.parseInt(conf.get("mapred.task.partition"));
        counter = 0; //how many ids in this mapper?
	}
	
	/**
	 * creates a new int key for each string entity id (initially a uri)
	 * apply cantor's pair function to create a unique id, based on
	 * the current mapper's id (number 1) and the current counter of the entity within the mapper (number 2)
	 * non-dbpedia get negative id 
	 * @param key the entity id
	 * @param value the attribute-value pairs of the entity,separated by "###"
	 * @param output key: a new entity id, value: the same as the input value
	 * 
	 */
	public void map(Text key, Text value,
			OutputCollector<VLongWritable, Text> output, Reporter reporter) throws IOException {

		String[] elements = value.toString().split("###"); //att-value pairs
		reporter.progress();
			
		if (elements.length < 2 || elements.length % 2 != 0) {
			reporter.incrCounter(InputData.MALFORMED_PAIRS, 1);
			System.out.println("Malformed: "+elements);
			return;
		}
		counter++;
		//use cantor's pairing function from currentMapper (number1) and local counter (number2)
		long newId = (((currentMapper+counter)*((long)(currentMapper+counter+1)))/2) + counter;
		if (newId < 0) { //in case the int is wrapped around
			newId = -newId;
			reporter.incrCounter(InputData.WRAPAROUND, 1);
		}
		if (key.charAt(0)!= '0') { //dbpedia entities start with 0;;;DBpediaURI
			reporter.incrCounter(InputData.DATASOURCE2, 1);
			newId = -newId; //assign negative ids to non-dbpedia datasources
		} else {
			reporter.incrCounter(InputData.DBPEDIA, 1);
		}
		output.collect(new VLongWritable(newId), value);
	}

}
