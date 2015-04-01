package hadoopUtils;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.hadoop.io.VIntWritable;

import preprocessing.VIntArrayWritable;

public class RelativePositionCompression {
	
	/**
	 * Returns a compression of the input set with length equal to input collection's size,
	 * containing all the elements of uncompressed as their difference to their previous element. <br/>
	 * Example: [1, 3, 14, 17, 25, 40] --> [1, 2, 11, 3, 8, 15]
	 * @param uncompressed the set to be compressed
	 * @return a compressed array representation of the uncompressed collection
	 */
	public static VIntArrayWritable compress(Collection<VIntWritable> uncompressed) {
		Set<VIntWritable> blocks = new TreeSet<>(uncompressed); //in case they are unordered
		
		VIntWritable[] compressed = new VIntWritable[blocks.size()];
		int i = 0;
		int prevBlock = 0;
		int currBlock;
		Iterator<VIntWritable> it = blocks.iterator();
		while (it.hasNext()) {			
			currBlock = it.next().get();
			compressed[i++] = new VIntWritable(currBlock - prevBlock);
			prevBlock = currBlock;
		}
		
		return new VIntArrayWritable(compressed);
	}
	
	/**
	 * Uncompresses the input set with length equal to input array's length,
	 * containing all the elements of compressed by adding the current compressed element to its previous uncompressed element. <br/>
	 * Example: [1, 2, 11, 3, 8, 15] --> [1, 3, 14, 17, 25, 40] 
	 * @param compressed the array to be uncompressed
	 * @return an uncompressed representation of compressed array
	 */
	public static VIntArrayWritable uncompress(VIntArrayWritable compressed) {	
		VIntWritable[] compressedArray = compressed.get();
		if (compressedArray.length < 1) {
			return null;
		}
		VIntWritable[] uncompressed = new VIntWritable[compressedArray.length];		
				
		int prevBlock = compressedArray[0].get();
		int currBlock;
		uncompressed[0] = new VIntWritable(prevBlock);
		for (int i = 1; i < compressedArray.length; ++i) {
			currBlock = prevBlock + compressedArray[i].get();
			prevBlock = currBlock;
			uncompressed[i] = new VIntWritable(currBlock);
		}
		
		return new VIntArrayWritable(uncompressed);
	}
	
	
	/**
	 * Uncompresses the input set with length equal to input array's length,
	 * containing all the elements of compressed by adding the current compressed element to its previous uncompressed element. <br/>
	 * Example: [1, 2, 11, 3, 8, 15] --> [1, 3, 14, 17, 25, 40] 
	 * @param compressed the array to be uncompressed
	 * @return an uncompressed representation of compressed array
	 */
	public static Integer[] uncompress(Integer[] compressed) {
		if (compressed.length < 1) {
			return null;
		}
		Integer[] uncompressed = new Integer[compressed.length];		
				
		Integer prevBlock = compressed[0];
		Integer currBlock;
		uncompressed[0] = prevBlock;
		for (int i = 1; i < compressed.length; ++i) {
			currBlock = prevBlock + compressed[i];
			prevBlock = currBlock;
			uncompressed[i] = currBlock;
		}
		
		return uncompressed;
	}
	
	
	/**
	 * Uncompresses the input set with length equal to input array's length,
	 * containing all the elements of compressed by adding the current compressed element to its previous uncompressed element. <br/>
	 * Example: [1, 2, 11, 3, 8, 15] --> [1, 3, 14, 17, 25, 40] 
	 * @param compressed the array to be uncompressed
	 * @return an uncompressed representation of compressed array
	 */
	public static VIntArrayWritable uncompress(Collection<Integer> compressed) {	
		Integer[] compressedArray = new Integer[compressed.size()]; 
		compressedArray = compressed.toArray(compressedArray);
		if (compressedArray.length < 1) {
			return null;
		}
		VIntWritable[] uncompressed = new VIntWritable[compressedArray.length];		
				
		int prevBlock = compressedArray[0];
		int currBlock;
		uncompressed[0] = new VIntWritable(prevBlock);
		for (int i = 1; i < compressedArray.length; ++i) {
			currBlock = prevBlock + compressedArray[i];
			prevBlock = currBlock;
			uncompressed[i] = new VIntWritable(currBlock);
		}
		
		return new VIntArrayWritable(uncompressed);
	}
	
	public static void main (String[] args) {
		Set<VIntWritable> blocks = new TreeSet<>();
		blocks.add(new VIntWritable(3));
		blocks.add(new VIntWritable(1));
		blocks.add(new VIntWritable(17));
		blocks.add(new VIntWritable(25));
		blocks.add(new VIntWritable(40));
		blocks.add(new VIntWritable(14));
		System.out.println(Arrays.toString(blocks.toArray()));
		
		VIntWritable[] compressed = compress(blocks).get();
		System.out.println(Arrays.toString(compressed));
		System.out.println(Arrays.toString(uncompress(new VIntArrayWritable(compressed)).get()));
		
		List<Integer> test = new ArrayList<>(compressed.length);
		for (int i = 0; i < compressed.length; ++i) {
			test.add(compressed[i].get());
		}
		System.out.println(Arrays.toString(uncompress(test).get()));
		
		Double testDouble = new Double(2) /3;
		System.out.println(testDouble);
		DecimalFormat df = new DecimalFormat("#.####");
		String doubleString = df.format(testDouble);
		System.out.println(doubleString);
		System.out.println(Double.parseDouble(doubleString));
		
		Double newDouble = new Double(2);
		System.out.println(Double.parseDouble(df.format(newDouble)));
		
	}

}
