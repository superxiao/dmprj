import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.SortedSet;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.text.StrTokenizer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.conf.Configuration;

public class FindingFrequentItemsets {
	static double supportThreshold = 0.0;
	static int totalLineNum = 0;
	static int subfileNum = 1;
	static String SET = "set";
	static String INDEX = "index";
	@SuppressWarnings("rawtypes")
	static List secondphaseset = new ArrayList();
	static HashSet<SortedSet<String>> secondPhaseCandidates;
	static String [] myargs = null;
	

	// Mapper class for the first phase MapReduce.
	// Will using static class and members cause re-entrance issue in mapreduce?
	public static class FirstMap extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		static String[] frequentItemTable;
		static HashMap<String, Integer> frequentItemIdxTable;
		static int[] pairCountTable;
		static List<HashMap<String, Integer>> itemsetCountTables;

		private static HashMap<String, Integer> getSingletonItemCounts(
				List<HashSet<String>> baskets) {
			HashMap<String, Integer> itemCountTable = new HashMap<String, Integer>();
			for (HashSet<String> basket : baskets) {
				// Possible empty item for consecutive spaces? May use a token
				// reader
				for (String item : basket) {
					if (!itemCountTable.containsKey(item)) {
						itemCountTable.put(item, 1);
					} else {
						itemCountTable.put(item, itemCountTable.get(item) + 1);
					}
				}
			}
			return itemCountTable;
		}

		private static int convertCountTableToFrequentItemIdxTable(
				HashMap<String, Integer> itemCountTable, double countThreshold) {
			int maxIdx = 0;
			for (Map.Entry<String, Integer> entry : itemCountTable.entrySet()) {
				if (entry.getValue() >= countThreshold)
					entry.setValue(maxIdx++);
				else
					entry.setValue(-1);
			}
			return maxIdx;
		}

		private String[] getFrequentItemTableFromFrequentItemIdxTable(
				HashMap<String, Integer> frequentItemIdxTable,
				int frequentItemCount) {
			String[] frequentItemTable = new String[frequentItemCount];
			for (Map.Entry<String, Integer> entry : frequentItemIdxTable
					.entrySet()) {
				if (entry.getValue() >= 0)
					frequentItemTable[entry.getValue()] = entry.getKey();
			}
			return frequentItemTable;
		}

		private static int getTriangularMatrixIdx(int n, int idxSmaller,
				int idxBigger) {
			// Convert 0-started index to 1-started
			idxSmaller++;
			idxBigger++;
			int idx = (idxSmaller - 1) * n - ((idxSmaller - 1) * idxSmaller)
					/ 2 + idxBigger - idxSmaller;
			// Convert back
			return idx - 1;
		}

		private static int[] countPairs(List<HashSet<String>> baskets,
				HashMap<String, Integer> frequentItemIdxTable,
				int frequentItemsCount) {
			// Use triangular matrix to count pairs.
			// Only count pairs whose individual elements are all frequent.
			int pairNumMax = frequentItemsCount * (frequentItemsCount - 1) / 2;
			int[] pairCountTable = new int[pairNumMax];
			for (HashSet<String> basket : baskets) {
				// Generate a list for frequent items in the basket.
				List<Integer> frequentItemsInBasket = getFrequentItemsIn(basket);
				for (int i = 0; i < frequentItemsInBasket.size(); i++) {
					for (int j = i + 1; j < frequentItemsInBasket.size(); j++) {
						int idx1 = frequentItemsInBasket.get(i);
						int idx2 = frequentItemsInBasket.get(j);
						int idxSmaller = Math.min(idx1, idx2);
						int idxBigger = Math.max(idx1, idx2);
						int idxInPairTable = getTriangularMatrixIdx(
								frequentItemsCount, idxSmaller, idxBigger);
						pairCountTable[idxInPairTable]++;
					}
				}
			}
			return pairCountTable;
		}
		
		private static List<Integer> getFrequentItemsIn(HashSet<String> basket) {
			List<Integer> frequentItems = new ArrayList<Integer>();
			for (String item : basket) {
				int idx = frequentItemIdxTable.get(item);
				if (idx >= 0)
					frequentItems.add(idx);
			}
			return frequentItems;
		}
		
		private static String formStrKeyFromItemset(Collection<Integer> itemset) {
			Integer[] itemsetArr = itemset.toArray(new Integer[itemset.size()]);
			Arrays.sort(itemsetArr);
			return StringUtils.join(itemsetArr, " ");
			
		}
		
		public static <T extends Comparable<T>> List<SortedSet<T>> getCkPlus1FromLk(List<SortedSet<T>> frequentItemsetsOf1less) {
			HashMap<SortedSet<T>, SortedSet<T>> LkMap = new HashMap<SortedSet<T>, SortedSet<T>>();
			for(SortedSet<T> itemset:frequentItemsetsOf1less) {
				SortedSet<T> key = new TreeSet<T>(itemset);
				T last = key.last();
				key.remove(key.last());
				if(LkMap.containsKey(key))
					LkMap.get(key).add(last);
				else {
					SortedSet<T> value = new TreeSet<T>();
					value.add(last);
					LkMap.put(key, value);
				}
			}
			List<SortedSet<T>> Ckp1 = new ArrayList<SortedSet<T>>();
			for(Map.Entry<SortedSet<T>, SortedSet<T>> entry:LkMap.entrySet()) {
				List<T> lastSet = new ArrayList<T>(entry.getValue());
				for(int i = 0; i < lastSet.size(); i++){
					for(int j = i+1; j < lastSet.size(); j++) {
						SortedSet<T> candidate = new TreeSet<T>(entry.getKey());
						candidate.add(lastSet.get(i));
						candidate.add(lastSet.get(j));
						Ckp1.add(candidate);
					}
				}
			}
			return Ckp1;
		}
		
		private static int getItemsetCount(SortedSet<Integer> itemset, int size) {
			if(size == 2) {
				Integer[] idx =itemset.toArray(new Integer[itemset.size()]);
				int idx1 = Math.min(idx[0], idx[1]);
				int idx2 = Math.max(idx[0], idx[1]);
				int idxInPairTable = getTriangularMatrixIdx(
						frequentItemTable.length, idx1, idx2);
				return pairCountTable[idxInPairTable];
			}
			else if(size > 2){
				String key = formStrKeyFromItemset(itemset);
				HashMap<String, Integer> countTable = itemsetCountTables.get(size-1);
				if(countTable.containsKey(key))
					return countTable.get(key);
				else {
					return 0;
				}
			}
			return -1;
		}
		
		// Get the list of frequent itemsets of given size in the give list of frequent items.
		private static void getFrequentItemsetsInAux(List<Integer> frequentItems, int currIdx, 
				int itemsetSize, Stack<Integer> itemStack, 
				double countThreshold, List<SortedSet<Integer>> frequentItemsets, HashSet<String> frequentStrItemsets){
			int currStackSize = itemStack.size();
			// Base case
			if(currStackSize == itemsetSize) {
				SortedSet<Integer> itemset = new TreeSet<Integer>(itemStack);
				int count = getItemsetCount(itemset, itemsetSize);
				if(count >= countThreshold) {
					String key = formStrKeyFromItemset(itemset);
					if(!frequentStrItemsets.contains(key)) {
						frequentStrItemsets.add(key);
						frequentItemsets.add(itemset);
					}
				}
				return;
			}
			// Recursion
			for(int i = currIdx; i <= frequentItems.size() - itemsetSize + currStackSize; i++) {
				int item = frequentItems.get(i);
				itemStack.push(item);
				getFrequentItemsetsInAux(frequentItems, i+1, itemsetSize, itemStack,
						countThreshold, frequentItemsets, frequentStrItemsets);
				itemStack.pop();
			}
		}
		
		// Get the list of frequent itemsets of given size in the basket.
		private static List<SortedSet<Integer>> getFrequentItemsetsIn(HashSet<String> basket, int itemsetSize, double countThreshold) {
			Stack<Integer> itemStack = new Stack<Integer>();
			List<SortedSet<Integer>> frequentPairs = new ArrayList<SortedSet<Integer>>();
			List<Integer> frequentItemsInBasket = getFrequentItemsIn(basket);
			HashSet<String> frequentStrItemsets = new HashSet<String>();
			getFrequentItemsetsInAux(frequentItemsInBasket, 0, 2, itemStack, countThreshold, frequentPairs, frequentStrItemsets);
			int currItemsetSize = 2;
			List<SortedSet<Integer>> candidates;
			List<SortedSet<Integer>> frequentItemsets = frequentPairs;
			while(currItemsetSize < itemsetSize) {
                candidates = getCkPlus1FromLk(frequentItemsets);
                frequentItemsets.clear();
                for(SortedSet<Integer> candidate:candidates) {
                		int count = getItemsetCount(candidate, currItemsetSize);
                		if(count >= countThreshold)
                			frequentItemsets.add(candidate);
                } 
                currItemsetSize++;
			}
			return frequentItemsets;
		}
		
		private static HashMap<String, Integer> countItemsetIn(List<HashSet<String>> baskets, int itemsetSize, double countThreshold) {
			HashMap<String, Integer> itemsetCountTable = new HashMap<String, Integer>();
			for(HashSet<String> basket : baskets) {
				List<SortedSet<Integer>> frequentItemsetsOf1less = getFrequentItemsetsIn(basket, itemsetSize-1, countThreshold);
				List<SortedSet<Integer>> candidates = getCkPlus1FromLk(frequentItemsetsOf1less);
				for(SortedSet<Integer> candidate:candidates) {
					String key =formStrKeyFromItemset(candidate);
					if(itemsetCountTable.containsKey(key))
						itemsetCountTable.put(key, itemsetCountTable.get(key)+1);
					else
						itemsetCountTable.put(key, 1);
				}
			}
			return itemsetCountTable;
		}
		
		@Override
		public void map(LongWritable key, Text value,
				Context output)
				throws IOException, InterruptedException {
			//System.out.println("Current block is \n" + value.toString());
			String[] lines = value.toString().split("\n");
			List<HashSet<String>> baskets = new ArrayList<HashSet<String>>(lines.length); 
			for(String line:lines) {
				String[] basket = line.split(" ");
				HashSet<String> basketSet = new HashSet<String>();
				for(String item:basket)
					basketSet.add(item);
				baskets.add(basketSet);
			}
			int basketNum = baskets.size();
			double countThreshold = supportThreshold * basketNum;
			HashMap<String, Integer> itemCountTable = getSingletonItemCounts(baskets);
			// This table starts at 1.
			int frequentItemCount = convertCountTableToFrequentItemIdxTable(
					itemCountTable, countThreshold);
			frequentItemIdxTable = itemCountTable;
			frequentItemTable = getFrequentItemTableFromFrequentItemIdxTable(
					frequentItemIdxTable, frequentItemCount);
			pairCountTable = countPairs(baskets, frequentItemIdxTable,
					frequentItemTable.length);
			
			itemsetCountTables = new ArrayList<HashMap<String, Integer>>();
			itemsetCountTables.add(null);
			itemsetCountTables.add(null);
			HashMap<String, Integer> countTable;
			int itemsetSize = 3;
			do {
				countTable = countItemsetIn(baskets, itemsetSize, countThreshold);
				itemsetCountTables.add(countTable);
				itemsetSize++;
			} while(countTable.size() != 0);
			
			// Output the frequent items
			IntWritable one = new IntWritable(1);
			for (String frequentItem : frequentItemTable) {
				//System.out.println("Collecting frequent item " + frequentItem);
				output.write(new Text(frequentItem), one);
			}
			// Output the frequent pairs
			for (int i = 0; i < frequentItemTable.length; i++) {
				for (int j = i + 1; j < frequentItemTable.length; j++) {

					// System.out.println("Frequent items count is "+frequentItemTable.length
					// + " i is " + i + " j is " + j);

					int triangleMatrixIdx = getTriangularMatrixIdx(
							frequentItemTable.length, i, j);
					//System.out.println("Triangle index is " + triangleMatrixIdx);
					if (pairCountTable[triangleMatrixIdx] >= countThreshold) {
						List<String> pairKey = new ArrayList<String>();
						pairKey.add(frequentItemTable[i]);
						pairKey.add(frequentItemTable[j]);
						Collections.sort(pairKey);
						output.write(
								new Text(StringUtils.join(pairKey, " ")), one);
					}
				}
			}
			
			// Output frequent triples
			for(HashMap<String, Integer> itemsetCountTable:itemsetCountTables) {
				if(itemsetCountTable == null) continue;
				for(Map.Entry<String, Integer> itemsetEntry:itemsetCountTable.entrySet()) {
					if(itemsetEntry.getValue()>=countThreshold) {
						String[] indices = itemsetEntry.getKey().split(" ");
						List<String> itemsetKey = new ArrayList<String>();
						for(String index:indices) {
							itemsetKey.add(frequentItemTable[Integer.parseInt(index)]);
						}
						Collections.sort(itemsetKey);
						output.write(new Text(StringUtils.join(itemsetKey, " ")), one);
					}
				}
			}
		}
	}

	public static class FirstReduce extends 
			Reducer<Text, IntWritable, Text, IntWritable> {
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context output)
				throws IOException, InterruptedException {
			output.write(key, new IntWritable(1));
		}
	}

	// the second phase
	public static class SecondMap extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		
		private static List<SortedSet<String>> getCandidateSingleItemsInBasket(List<String> basket) {
			List<SortedSet<String>> candidates = new ArrayList<SortedSet<String>>();
			for(String item:basket) {
				SortedSet<String> itemset = new TreeSet<String>();
				itemset.add(item);
				if(secondPhaseCandidates.contains(itemset))
					candidates.add(itemset);
			}
			return candidates;
		}
		
		private static List<SortedSet<String>> getCandidateItemsetsInBasket(List<String> basket) {
			List<SortedSet<String>> candidateItemsets = new ArrayList<SortedSet<String>>();
			List<SortedSet<String>> candidateSingles = getCandidateSingleItemsInBasket(basket);
			candidateItemsets.addAll(candidateSingles);
			List<SortedSet<String>> candidatePairs = new ArrayList<SortedSet<String>>();
			for(int i = 0; i < candidateSingles.size(); i++) {
				for(int j = i + 1; j < candidateSingles.size(); j++) {
					SortedSet<String> pair = new TreeSet<String>();
					pair.add(candidateSingles.get(i).first());
					pair.add(candidateSingles.get(j).first());
					if(secondPhaseCandidates.contains(pair)) {
						candidateItemsets.add(pair);
						candidatePairs.add(pair);
					}
				}
			}
			List<SortedSet<String>> candidateKs = candidatePairs;
			while(candidateKs.size() != 0) {
				candidateKs =  FirstMap.getCkPlus1FromLk(candidateKs);
				List<SortedSet<String>> filteredCandidateKs = new ArrayList<SortedSet<String>>();
				for(SortedSet<String> candidate:candidateKs) {
					if(secondPhaseCandidates.contains(candidate)) {
						candidateItemsets.add(candidate);
						filteredCandidateKs.add(candidate);
					}
					candidateKs = filteredCandidateKs;
				}
			}
			
			return candidateItemsets;
		}
		
		@Override
		public void map(LongWritable key, Text value,
				Context output)
				throws IOException, InterruptedException {
			String[] baskets = value.toString().split("\n");
			int n = baskets.length;
			List<String> basket = new ArrayList<String>();
			Map<SortedSet<String>, Integer> candidateCountTable = new HashMap<SortedSet<String>, Integer>();
			// using two loops to count the frequent of every candidate subset.
			for (int i = 0; i < n; i++) {
				basket = new StrTokenizer(baskets[i]).getTokenList();
				List<SortedSet<String>> candidatesInBasket = getCandidateItemsetsInBasket(basket);
				for (SortedSet<String> candidate:candidatesInBasket) {	
					if (candidateCountTable.containsKey(candidate)) {
						Integer count = candidateCountTable.get(candidate);
						count += 1;
						candidateCountTable.put(candidate, count);
					} else {
						candidateCountTable.put(candidate, 1);
					}
				}
			}
			for (SortedSet<String> candidate : candidateCountTable.keySet()) {
				int count = candidateCountTable.get(candidate);
				Text set = new Text(StringUtils.join(candidate, " "));
				output.write(set, new IntWritable(count));
			}
		}
	}

	public static class Second_Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values,
				Context output)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable value:values) {
				sum += value.get();
			}
			if (sum >= supportThreshold * totalLineNum)
				output.write(key, new IntWritable(sum));
		}
	}
	
	//pre processing
    public static void preprocessingphase1(String[] args)  throws Exception{
    	//making the temp input files.
    			String originalfilepath = FileProcessing.getLocation(args[0]);
    			System.out.println(originalfilepath);
    			if (originalfilepath == null) return;
    			List<String> lines = FileProcessing.readFile(originalfilepath);
    			if (lines == null) return;
    			totalLineNum = lines.size();
    			
    			 subfileNum = Integer.parseInt(args[1]);
    			int m = (int) totalLineNum/subfileNum;
    			double m_d = totalLineNum*1.0/subfileNum;
    			if (m_d > m) m = m + 1;
    			FileProcessing.mkdir("input_temp");
    			for (int i = 0; i < subfileNum; i++){
    				String newpath = "input_temp/"+i+".dat";
    				StringBuilder input_temp = new StringBuilder();
    				for (int j = 0; j < m && totalLineNum - i*m - j  > 0; j++){
    					input_temp.append(lines.get(i*m+j)).append("\n");
    				}
    				FileProcessing.createFile(newpath, input_temp.toString().getBytes());
    		}
    }
    
    //pre processing for phase 2
    public static void preprocessingphase2() throws Exception{
    	secondPhaseCandidates = new HashSet<SortedSet<String>>(); 
    	List<String> lines = FileProcessing.readFile("output_temp/part-r-00000");
    	for(String line:lines) {
    		List<String> itemset = new StrTokenizer(line).getTokenList();
    		itemset.remove(itemset.size()-1);
    		secondPhaseCandidates.add(new TreeSet<String>(itemset));	
    	}
    	System.out.println("Pre processing for phase 2 finished.");
    }
    
    //final process
    public  static void finalprocess() throws Exception{
       	secondphaseset.clear();
    	List<String> lines = FileProcessing.readFile("output/part-r-00000");
    	for (Iterator i = lines.iterator(); i.hasNext();){
    		String str = (String) i.next();
    		List temp = Arrays.asList(str.split("\t"));
    		secondphaseset.add(temp);
    	}
    	Collections.sort(secondphaseset, new Comparator<List>(	) {
			@Override
			public int compare(List a, List b) {
				int aa = Integer.parseInt((String)a.get(1));
		    	int bb = Integer.parseInt((String)b.get(1));
		    	int value = 0;
		    	if (aa < bb) value =  1;
		    	if (aa > bb) value =  -1;
		    	if (aa == bb) {
		    		String aaa = (String) a.get(0);
		    		String bbb = (String) b.get(0);
		    		value =  aaa.compareTo(bbb);
		    	}
		    	return value;
			}
    		
		});
    	@SuppressWarnings("unused")
		String str_finial = "";
    	for (Iterator i = secondphaseset.iterator(); i.hasNext();){
    		List tmp = (List) i.next();
    		String str = tmp.get(0)+" ("+tmp.get(1)+")\n";
    		str_finial += str;
    	}
    	FileProcessing.mkdir("result");
    	String str = ""+secondphaseset.size()+"\n";
    	FileProcessing.createFile("result/result.txt", str.getBytes());
    	FileProcessing.appendToFile("result/result.txt",str_finial);
    	System.out.println("All finished.");
    }
    //main function
	public  void 	run(String[] args){
		if (args.length < 3){
			System.out.println("The number of arguments is less than three.");
			return;
		}
		//first phase
		try {
			preprocessingphase1(args);
		} catch (Exception e) {
			System.out.println("Preprccessing Phase 1 fail");
			e.printStackTrace();
		}
		try {
			phase1(args);
		} catch (Exception e) {
			System.out.println("Phase 1 fail");
			e.printStackTrace();
		}
		
		//second phase
		try {
			preprocessingphase2();
		} catch (Exception e) {
			System.out.println("Preprccessing Phase 2 fail");
			e.printStackTrace();
		}
		try {
			phase2(args);
		} catch (Exception e) {
			System.out.println("Phase 2 fail");
			e.printStackTrace();
		}
		//final process
		try {
			finalprocess();
		} catch (Exception e) {
			System.out.println("Finalprocess fail");
			e.printStackTrace();
		}
	}

	// phase 1
	public static void phase1(String[] args) throws Exception {
		supportThreshold = Double.parseDouble(args[2]);
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Finding Frequent Itemsets");
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(FirstMap.class);
		// conf.setCombinerClass(First_Reduce.class);
		job.setReducerClass(FirstReduce.class);
		job.setInputFormatClass(TextInputFormatWithWholeFileRecords.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.setInputPaths(job, new Path("input_temp"));
		FileOutputFormat.setOutputPath(job, new Path("output_temp"));
		job.waitForCompletion(true);
	}

	// phase 2
	public static void phase2(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Finding Frequent Itemsets");
		job.setJobName("Frequent Itemsets Count");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(SecondMap.class);
		// conf.setCombinerClass(First_Reduce.class);
		job.setReducerClass(Second_Reduce.class);
		job.setInputFormatClass(TextInputFormatWithWholeFileRecords.class);
		FileInputFormat.setInputPaths(job, new Path("input_temp"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		job.waitForCompletion(true);
	}
}

