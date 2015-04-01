package blockingGraphBuilding;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.VIntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class EJSMapper extends MapReduceBase implements Mapper<Text, VIntWritable, Text, Text> {

	Text i_Bi = new Text();
	Text j_Bj = new Text();
	/**
	 * @param key i,|Bi|,j,|Bj| where i,j are enity ids and |Bi|,|Bj| the respective entity index sizes
	 * @param value 1 (ignore)
	 * @param output key: i,|Bi| value: j,|Bj| and also the reverse (key:j,|Bj| value: i,|Bi|)	  
	 */
	public void map(Text key, VIntWritable value,
			OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
		String[] keyString = key.toString().split(",");		
		
		i_Bi.set(keyString[0]+","+keyString[1]);		
		j_Bj.set(keyString[2]+","+keyString[3]);		
		output.collect(i_Bi,j_Bj);
		output.collect(j_Bj,i_Bi);		
	}

}
