package hadoopUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.io.VIntWritable;


public class MBTools {
	
	public enum WeightingScheme {CBS, ECBS, ARCS, JS, EJS}; 
	
	/**
	 * the inverse of Arrays.toString(), returning a list
	 * @param stringArray the result of Arrays.toString()
	 * @return the list represenation of the array that was given as parameter in the original Arrays.toString() method
	 */
	public static List<Integer> listFromStringArray(String stringArray) {
		List<Integer> result = new ArrayList<>();		
		String[] blocks = stringArray.substring(1, stringArray.lastIndexOf("]")).split(", ");	    
	    for (String block : blocks) {
	    	result.add(Integer.parseInt(block));	      
	    }
	    return result;
	}

	public static boolean isRepeated(List<Integer> blocks1, List<Integer> blocks2, int blockIndex) {		
		 for (int i = 0; i < blocks1.size(); ++i) {
			 int block1i = blocks1.get(i);
			 if (i == 0 && block1i == blockIndex) { //first block in index => cannot be repeated
				 return false;
			 }
			 for (int j = 0; j < blocks2.size(); ++j) {
				 int block2j = blocks2.get(j);
				 if (j == 0 && block2j == blockIndex) { //first block in index => cannot be repeated
					 return false;
				 }
				 if (block2j < block1i) {
					 continue;
				 }
				 if (block1i < block2j) {
					 break;
				 }
				 if (block1i == block2j) {
					 return block1i != blockIndex;
				 }
			 }
		 }
		 //System.err.println("Error!!!!"); //since this check comes from a common block
		 return false;
	 } 
	
	public static boolean isRepeated(List<Integer> blocks1, List<Integer> blocks2, int blockIndex, String weightingScheme) {
		if (weightingScheme.equals("ARCS")) {
			List<Integer> newBlocks1 = new ArrayList<>(blocks1.size()/2);
			for (int i = 0; i < blocks1.size(); i+=2) {
				newBlocks1.add(blocks1.get(i));
			}
			List<Integer> newBlocks2 = new ArrayList<>(blocks2.size()/2);
			for (int i = 0; i < blocks2.size(); i+=2) {
				newBlocks2.add(blocks2.get(i));
			}
			return isRepeated(newBlocks1, newBlocks2, blockIndex);
		} else if (weightingScheme.equals("EJS")) {
			//TODO: fill this code
			assert(false); //not implemented yet
			return isRepeated(blocks1, blocks2, blockIndex);
		} else {
			return isRepeated(blocks1, blocks2, blockIndex);   
		}
	 } 
	
	 public static boolean isRepeated(Integer[] blocks1, Integer[] blocks2, int blockIndex) {		
		 for (int i = 0; i < blocks1.length; ++i) {
			 Integer block1i = blocks1[i];
			 if (i == 0 && block1i == blockIndex) { //first block in index => cannot be repeated
				 return false;
			 }
			 for (int j = 0; j < blocks2.length; ++j) {
				 Integer block2j = blocks2[j];
				 if (j == 0 && block2j == blockIndex) { //first block in index => cannot be repeated
					 return false;
				 }
				 if (block2j < block1i) {
					 continue;
				 }
				 if (block1i < block2j) {
					 break;
				 }
				 if (block1i == block2j) {
					 return block1i != blockIndex;
				 }
			 }
		 }
		 //System.err.println("Error!!!!"); //since this check comes from a common block
		 assert(false) : "Error!No common blocks for two entities of the same block!";
		 return false;
	 } 
	
	public static double getWeight(List<Integer> blocks1, List<Integer> blocks2, int blockIndex, String weightingScheme) {
		return getWeight(blocks1, blocks2, blockIndex, weightingScheme,	0, 0);
	}

	public static double getWeight(List<Integer> blocks1, List<Integer> blocks2, int blockIndex, String weightingScheme, int totalBlocks) {		
		return getWeight(blocks1, blocks2, blockIndex, weightingScheme,	totalBlocks, 0);
	}

	public static double getWeight(List<Integer> blocks1, List<Integer> blocks2, int blockIndex, String weightingScheme, int totalBlocks, long validComparisons) {
		 switch (weightingScheme) {
		 case "ARCS":						 
			 final Map<Integer,Integer> commonIndices = getCommonBlockIndicesARCS(blocks1, blocks2);
			 if (commonIndices == null) {
				 return -1;
			 }
			 double totalWeight = 0;
			 for (Map.Entry<Integer, Integer> commonBlock : commonIndices.entrySet()) {
				 totalWeight += 1.0 / commonBlock.getValue();
			 }
			 return totalWeight;
		 case "CBS":
			 return getNoOfCommonBlocks(blocks1, blocks2);
		 case "ECBS":
			 double commonBlocks = getNoOfCommonBlocks(blocks1, blocks2);
			 if (commonBlocks < 0) {
				 return commonBlocks;
			 }
		 	 return commonBlocks * Math.log10((double)totalBlocks / blocks1.size()) * Math.log10((double)totalBlocks / blocks2.size());
		 case "JS":
			 double commonBlocksJS = getNoOfCommonBlocks(blocks1, blocks2);
			 if (commonBlocksJS < 0) {
				 return commonBlocksJS;
			 }
		 	 return  commonBlocksJS / (blocks1.size() + blocks2.size() - commonBlocksJS);
		 case "EJS":
			 List<Integer> actualBlocksE1 = blocks1.subList(0, blocks1.size()-1); //the last value is the cardinality of e1
			 List<Integer> actualBlocksE2 = blocks2.subList(0, blocks2.size()-1); //the last value is the cardinality of e2
			 int e1Comparisons = blocks1.get(blocks1.size()-1);
			 int e2Comparisons = blocks2.get(blocks2.size()-1);
			 double commonBlocksEJS = getNoOfCommonBlocks(actualBlocksE1, actualBlocksE2);
		 	 if (commonBlocksEJS < 0) {
		 		 return commonBlocksEJS;
		 	 }
		 	 double probability = commonBlocksEJS / (actualBlocksE1.size() + actualBlocksE2.size() - commonBlocksEJS);
		 	 return probability * Math.log10(validComparisons / e1Comparisons) * Math.log10(validComparisons / e2Comparisons);
		 }
		 return -1;
	}
	 
	private static Map<Integer, Integer> getCommonBlockIndicesARCS(List<Integer> blocks1, List<Integer> blocks2) {		
		Map<Integer,Integer> newBlocks1 = new HashMap<>();
		for (int i = 0; i < blocks1.size(); i+=2) {
			newBlocks1.put(blocks1.get(i), blocks1.get(i+1));
		}
		Map<Integer,Integer> newBlocks2 = new HashMap<>();
		for (int i = 0; i < blocks2.size(); i+=2) {
			newBlocks2.put(blocks2.get(i), blocks2.get(i+1));
		}		
		newBlocks1.entrySet().retainAll(newBlocks2.entrySet());
		return newBlocks1;
	}

	public static Set<Integer> getCommonBlockIndices(List<Integer> blocks1, List<Integer> blocks2) {
		Set<Integer> intersection = new HashSet<>(blocks1);
		intersection.retainAll(blocks2);
		return intersection;
		
		/*List<Integer> blocks1 = entityIndex.get(e1);
		List<Integer> blocks2 = entityIndex.get(e2);
		boolean firstCommonIndex = false;
		int noOfBlocks1 = blocks1.size();
		int noOfBlocks2 = blocks2.size();
		final List<Integer> indices = new ArrayList<Integer>();
		for (int i = 0; i < noOfBlocks1; i++) {
			for (int j = 0; j < noOfBlocks2; j++) {
				if (blocks2.get(j) < blocks1.get(i)) {
					continue;
				}
				if (blocks1.get(i) < blocks2.get(j)) {
					break;
				}
				if (blocks1.get(i) == blocks2.get(j)) {
					if (!firstCommonIndex) {
						firstCommonIndex = true;
						if (blocks1.get(i) != blockIndex) {
							return null;
						}
					}
					indices.add(blocks1.get(i));
				}
			}
		}
		return indices;*/
	}
	
	public static int getNoOfCommonBlocks(List<Integer> blocks1, List<Integer> blocks2) {
		
		return getCommonBlockIndices(blocks1, blocks2).size();
		
        /*
        boolean firstCommonIndex = false;
        int commonBlocks = 0;
        int noOfBlocks1 = blocks1.size();
        int noOfBlocks2 = blocks2.size();
        for (int i = 0; i < noOfBlocks1; i++) {
        	int block1i = blocks1.get(i);
            for (int j = 0; j < noOfBlocks2; j++) {
            	int blocks2j = blocks2.get(j);
                if (blocks2j < block1i) {
                    continue;
                }

                if (block1i < blocks2j) {
                    break;
                }

                if (block1i == blocks2j) {
                    commonBlocks++;
                    if (!firstCommonIndex) {
                        firstCommonIndex = true;
                        if (block1i != blockIndex) {
                            return -1;
                        }
                    }
                }
            }
        }
        
        return commonBlocks;
        */
    }
	
	/*public static int getNoOfEntityBlocks(Map<Integer,List<Integer>> entityIndex, int entityId) {
		List<Integer> entityBlocks;
        if ((entityBlocks = entityIndex.get(entityId)) == null) {
            return -1;
        }
        
        return entityBlocks.size();
    }*/
	
	public static void main (String[] args) {		
		List<VIntWritable> blocks = new ArrayList<>(); 
		int comparisons = 9999;					

		blocks.add(new VIntWritable(1));
		blocks.add(new VIntWritable(1123123));
		blocks.add(new VIntWritable(234234));
		
		VIntWritable[] toEmitArray = new VIntWritable[blocks.size()+2];
		toEmitArray[0] = new VIntWritable(0); 													//first index->Eid
		System.arraycopy(blocks.toArray(), 0, toEmitArray, 1, blocks.size());	//middle ->block
		toEmitArray[blocks.size()+1] = new VIntWritable(comparisons);
		System.out.println(Arrays.toString(toEmitArray));
	}

}
