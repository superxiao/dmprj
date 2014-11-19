import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Collections;
import java.util.Set;
import java.util.SortedSet;
import java.util.Stack;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

<<<<<<< HEAD:src/FindingFrequentItemsets.java
public class FindingFrequentItemset {
=======
public class FrequentItemset {
>>>>>>> fb31f0088fc4333a441c9239980889dd8bbb5872:src/FrequentItemset.java
	static double supportThreshold = 0.0;
	static int totalLineNum = 0;
	static int subfileNum = 1;
	static String SET = "set";
	static String INDEX = "index";
	@SuppressWarnings("rawtypes")
	static List<List> firstphaseset = new ArrayList<List>();
	static List secondphaseset = new ArrayList();
<<<<<<< HEAD:src/FindingFrequentItemsets.java
	static String [] myargs = null;
	
=======

>>>>>>> fb31f0088fc4333a441c9239980889dd8bbb5872:src/FrequentItemset.java
	// Mapper class for the first phase MapReduce.
	// Will using static class and members cause re-entrance issue in mapreduce?
	public static class FirstMap extends
			Mapper<LongWritable, Text, Text, IntWritable> {

		static String[] frequentItemTable;
		static HashMap<String, Integer> frequentItemIdxTable;
		static int[] pairCountTable;
		static List<HashMap<String, Integer>> itemsetCountTables;

		private static HashMap<String, Integer> getSingletonItemCounts(
				List<SortedSet<String>> baskets) {
			HashMap<String, Integer> itemCountTable = new HashMap<String, Integer>();
			for (SortedSet<String> basket : baskets) {
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

		private static int[] countPairs(List<SortedSet<String>> baskets,
				HashMap<String, Integer> frequentItemIdxTable,
				int frequentItemsCount) {
			int p = 1;
			// Use triangular matrix to count pairs.
			// Only count pairs whose individual elements are all frequent.
			int pairNumMax = frequentItemsCount * (frequentItemsCount - 1) / 2;
			int[] pairCountTable = new int[pairNumMax];
			for (SortedSet<String> basket : baskets) {
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
						if(idxInPairTable < 0)
							p = 1;
						pairCountTable[idxInPairTable]++;
					}
				}
			}
			return pairCountTable;
		}
		
		private static List<Integer> getFrequentItemsIn(SortedSet<String> basket) {
			List<Integer> frequentItems = new ArrayList<Integer>();
			for (String item : basket) {
				int idx = frequentItemIdxTable.get(item);
				if (idx >= 0)
					frequentItems.add(idx);
			}
			return frequentItems;
		}
		
		private static SortedSet<Integer> joinSetsWithOneDiff(SortedSet<Integer> set1, SortedSet<Integer> set2) {
			if(set1.size() != set2.size()) return null;
			TreeSet<Integer> joined = new TreeSet<Integer>();
			joined.addAll(set1);
			joined.addAll(set2);
			if(joined.size() - set1.size() == 1)
				return joined;
			else
				return null;
		}
		
		private static List<SortedSet<Integer>> getCkPlus1FromLk(List<SortedSet<Integer>> Lk) {
			HashSet<String> Ckp1Hash = new HashSet<String>();
			List<SortedSet<Integer>> Ckp1 = new ArrayList<SortedSet<Integer>>();
			for(int i = 0; i < Lk.size(); i++) {
				for(int j = i + 1; j < Lk.size(); j++) {
					SortedSet<Integer> joined = joinSetsWithOneDiff(Lk.get(i), Lk.get(j));
					if(joined != null){
						String tripleString = StringUtils.join(joined, " ");
						if(!Ckp1Hash.contains(tripleString))
						{
							Ckp1.add(joined);
							Ckp1Hash.add(tripleString);
						}
					}
				}
			}
			return Ckp1;
		}
		
		private static int getItemsetCount(SortedSet<Integer> itemset, int size) {
			if(size == 2) {
				int idxInPairTable = getTriangularMatrixIdx(
						frequentItemTable.length, itemset.first(), itemset.last());
				return pairCountTable[idxInPairTable];
			}
			else if(size > 2){
				String key = StringUtils.join(itemset, " ");
				HashMap<String, Integer> countTable = itemsetCountTables.get(size-1);
				return countTable.get(key);
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
				SortedSet<Integer> itemsetSorted = new TreeSet<Integer>(itemStack);
				int count = getItemsetCount(itemsetSorted, itemsetSize);
				if(count >= countThreshold) {
					String key = StringUtils.join(itemsetSorted, " ");
					if(!frequentStrItemsets.contains(key)) {
						frequentStrItemsets.add(key);
						frequentItemsets.add(itemsetSorted);
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
		private static List<SortedSet<Integer>> getFrequentItemsetsIn(SortedSet<String> basket, int itemsetSize, double countThreshold) {
			Stack<Integer> itemStack = new Stack<Integer>();
			List<SortedSet<Integer>> frequentItemsets = new ArrayList<SortedSet<Integer>>();
			List<Integer> frequentItemsInBasket = getFrequentItemsIn(basket);
			HashSet<String> frequentStrItemsets = new HashSet<String>();
			getFrequentItemsetsInAux(frequentItemsInBasket, 0, itemsetSize, itemStack, countThreshold, frequentItemsets, frequentStrItemsets);
			return frequentItemsets;
		}
		
		private static HashMap<String, Integer> countItemsetIn(List<SortedSet<String>> baskets, int itemsetSize, double countThreshold) {
			HashMap<String, Integer> itemsetCountTable = new HashMap<String, Integer>();
			for(SortedSet<String> basket : baskets) {
				List<SortedSet<Integer>> frequentItemsetsOf1less = getFrequentItemsetsIn(basket, itemsetSize-1, countThreshold);
				List<SortedSet<Integer>> candidates = getCkPlus1FromLk(frequentItemsetsOf1less);
				for(SortedSet<Integer> candidate:candidates) {
					String key = StringUtils.join(candidate, " ");
					if(itemsetCountTable.containsKey(key))
						itemsetCountTable.put(key, itemsetCountTable.get(key)+1);
					else
						itemsetCountTable.put(key, 1);
				}
			}
			return itemsetCountTable;
		}
		
		public void map(LongWritable key, Text value,
				Context output)
				throws IOException, InterruptedException {
			System.out.println("Current block is \n" + value.toString());
			String[] lines = value.toString().split("\n");
			List<SortedSet<String>> baskets = new ArrayList<SortedSet<String>>(lines.length); 
			for(String line:lines) {
				String[] basket = line.split(" ");
				SortedSet<String> basketSet = new TreeSet<String>();
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
				System.out.println("Collecting frequent item " + frequentItem);
				output.write(new Text(frequentItem), one);
			}
			// Output the frequent pairs
			for (int i = 0; i < frequentItemTable.length; i++) {
				for (int j = i + 1; j < frequentItemTable.length; j++) {

					// System.out.println("Frequent items count is "+frequentItemTable.length
					// + " i is " + i + " j is " + j);

					int triangleMatrixIdx = getTriangularMatrixIdx(
							frequentItemTable.length, i, j);
					System.out
							.println("Triangle index is " + triangleMatrixIdx);
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
					if(itemsetEntry.getValue()>countThreshold) {
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
		public void reduce(Text key, Iterator<IntWritable> values,
				Context output)
				throws IOException, InterruptedException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
				// values.next().get();
			}
			// output.collect(key, new IntWritable(sum));
			output.write(key, new IntWritable(1));
		}
	}

	// the second phase
	public static class SecondMap extends
			Mapper<LongWritable, Text, Text, IntWritable> {
		public void map(LongWritable key, Text value,
				Context output)
				throws IOException, InterruptedException {
			String data = value.toString();
			String[] baskets = data.split("\n");
			int n = baskets.length;
			List<String> basket_set = new ArrayList<String>();
			Map<List, Integer> frequent_itemset_count = new HashMap<List, Integer>();
			// using two loops to count the frequent of every candidate subset.
			for (int i = 0; i < n; i++) {
				String[] basket = baskets[i].split(" ");
				int m = basket.length;
				basket_set.clear();
				for (int j = 0; j < m; j++)
					basket_set.add(basket[j]);
				m = firstphaseset.size();
				for (Iterator j = firstphaseset.iterator(); j.hasNext();) {
					List s = new ArrayList((List) j.next());
					if (basket_set.containsAll(s)) {
						if (frequent_itemset_count.containsKey(s)) {
							Integer v = frequent_itemset_count.get(s);
							v += 1;
							frequent_itemset_count.put(s, v);
						} else {
							frequent_itemset_count.put(s, new Integer(1));
						}
					}
				}
			}
			for (List candidate : frequent_itemset_count.keySet()) {
				String str_temp = "";
				int m = candidate.size();
				int count = frequent_itemset_count.get(candidate);
				for (int i = 0; i < m; i++) {
					if (i == 0)
						str_temp += candidate.get(i);
					else
						str_temp += " " + candidate.get(i);
				}
				Text set = new Text(str_temp);
				output.write(set, new IntWritable(count));
			}
		}
	}

	public static class Second_Reduce extends
			Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values,
				Context output)
				throws IOException, InterruptedException {
			int sum = 0;
			while (values.hasNext()) {
				sum += values.next().get();
			}
			if (sum >= supportThreshold * totalLineNum)
				output.write(key, new IntWritable(sum));
		}
	}
<<<<<<< HEAD:src/FindingFrequentItemsets.java
	
	//pre processing
    public static void preprocessingphase1(String[] args)  throws Exception{
    	//making the temp input files.
    			String originalfilepath = FileProcessing.getLocation(args[0]);
    			System.out.println(originalfilepath);
    			if (originalfilepath == null) return;
    			List<String> lines = FileProcessing.readFile(originalfilepath);
    			if (lines == null) return;
    			total = lines.size();
    			
    			 partition = Integer.parseInt(args[1]);
    			int m = (int) total/partition;
    			double m_d = total*1.0/partition;
    			if (m_d > m) m = m + 1;
    			FileProcessing.mkdir("input_temp");
    			for (int i = 0; i < partition; i++){
    				String newpath = "input_temp/"+i+".dat";
    				String input_temp = "";
    				for (int j = 0; j < m && total - i*m - j  > 0; j++){
    					input_temp += lines.get(i*m+j)+"\n";
    				}
    				FileProcessing.createFile(newpath, input_temp.getBytes());
    				//appendToFile(newpath, input_temp);
   			}
    }
    
    //phase 1
    public static void phase1(String[] args) throws Exception{
    	// What's args[2]?
    	supportThreshold = Double.parseDouble(args[2]);
		JobConf conf = new JobConf(FrequentItemset.class);
		conf.setJobName("Finding Frequent Itemsets");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(FirstMap.class);
		//conf.setCombinerClass(First_Reduce.class);
		conf.setReducerClass(FirstReduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		FileInputFormat.setInputPaths(conf, new Path("input_temp"));
		FileOutputFormat.setOutputPath(conf, new Path("output_temp"));
		JobClient.runJob(conf);
    }
    
    //pre processing for phase 2
    public static void preprocessingphase2(String[] args) throws Exception{
    	List<String> lines = FileProcessing.readFile("output_temp/part-00000");
    	Iterator<String> itr = lines.iterator();
    	while (itr.hasNext()) {
    	    String basket = (String) itr.next();
    	    String[] items = basket.split(" |\t");
    	    List<String> temp = new ArrayList<String>();
    	    Collections.addAll(temp, items); 
    	    int n = temp.size();
    	    temp.remove(n-1);
    	    firstphaseset.add(temp);
    	}
    	//
    	//System.out.println(firstphaseset);
    	System.out.println("Pre processing for phase 2 finished.");
    }
    //phase 2
    public static void phase2(String[] args) throws Exception{
    	JobConf conf = new JobConf(FindingFrequentItemsets.class);
		conf.setJobName("Frequent Itemsets Count");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(SecondMap.class);
		//conf.setCombinerClass(First_Reduce.class);
		conf.setReducerClass(Second_Reduce.class);
		FileInputFormat.setInputPaths(conf, new Path("input_temp"));
		FileOutputFormat.setOutputPath(conf, new Path("output"));
		JobClient.runJob(conf);
    }
    //the merge sort procedure
    public static void merge_sort(){
    	int l = 0;
    	int r = secondphaseset.size()-1;
    	int len=r-l+1;
    	//枚举的是一半的长度 
       for(int i=1;i<=len;i*=2){
 	        int left=l;
	        while(left<r){
			    int mid=left+i-1;
			    int right=left+2*i-1;
			    //中间值大于右边界，说明排好序了 
			    if(mid>r) break;
			    //中间值没有超，右边界超了 
			    if(right>r) right=r;
			    //mid和right相等的时候，也不需要排序 
			    if(right==mid) break;
			    List temp = new ArrayList();
			    merge(left,mid,right, temp);
			    left=right+1; 
    		}
    	}
    }
	public static boolean smaller(List a, List b){
    	int aa = Integer.parseInt((String)a.get(1));
    	int bb = Integer.parseInt((String)b.get(1));
    	int value = 0;
    	if (aa < bb) value =  -1;
    	if (aa > bb) value =  1;
    	if (aa == bb) {
    		String aaa = (String) a.get(0);
    		String bbb = (String) b.get(0);
    		value =  - aaa.compareTo(bbb);
    	}
    	if (value < 0) return false;
    	else return true;
    }
   public static void merge(int left, int middle, int right, List temp){
    	int i = left, j = middle+1;
    	int m = middle, n = right;
    	int k = 0;
    	while(i <= m && j <= n){
    		if (smaller((List)secondphaseset.get(i), (List)secondphaseset.get(j))){
    			temp.add(secondphaseset.get(i));
    			i++;
    		}else{
    			temp.add(secondphaseset.get(j));
    			j++;
    		}
    	}
    	while(i <=m) {temp.add(secondphaseset.get(i));i++;}
    	while(j <=n) {temp.add(secondphaseset.get(j));j++;}
    	k = temp.size();
    	for (int v  = 0; v < k; v ++) secondphaseset.set(left+v, temp.get(v));
    }
    //final process
    public  static void finalprocess() throws Exception{
    	FileProcessing.deleteFile("output_temp");
    	FileProcessing.deleteFile("input_temp");
    	secondphaseset.clear();
    	List<String> lines = FileProcessing.readFile("output/part-00000");
    	for (Iterator i = lines.iterator(); i.hasNext();){
    		String str = (String) i.next();
    		List temp = Arrays.asList(str.split("\t"));
    		secondphaseset.add(temp);
    	}
    	List temp = new ArrayList();
    	merge_sort();
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
			preprocessingphase2(args);
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
=======

	// Create a directory in the DFS file system
	public static void mkdir(String dirPath) throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path srcPath = new Path(dirPath);
		boolean isok = fs.mkdirs(srcPath);
		if (isok) {
			System.out.println("Directory " + dirPath + " created in the DFS.");
		} else {
			System.out.println("Failed to create directory in the DFS.");
		}
		fs.close();
	}

	// Create a file in the DFS file system
	public static void createFile(String dst, byte[] contents)
			throws IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		Path dstPath = new Path(dst);
		FSDataOutputStream outputStream = fs.create(dstPath);
		outputStream.write(contents);
		outputStream.close();
		fs.close();
		System.out.println("file " + dst + " created in DFS.");
	}

	// 追加写入文件
	private static void appendToFile(String dst, String line)
			throws FileNotFoundException, IOException {
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataOutputStream outputStream = fs.append(new Path(dst));
		outputStream.write(line.getBytes());
		/*
		 * int readLen = line.getBytes().length; while(-1 != readLen){
		 * out.write("zhangzk add by hdfs java api".getBytes(), 0, readLen); }
		 */
		outputStream.close();
		fs.close();
	}

	// 读取文件的内容
	public static List<String> readFile(String filePath) throws IOException {
		Path f = new Path(filePath);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(filePath), conf);
		FSDataInputStream dis = fs.open(f);
		InputStreamReader isr = new InputStreamReader(dis, "utf-8");
		BufferedReader br = new BufferedReader(isr);
		List<String> lines = new ArrayList<String>();
		String str = "";
		while ((str = br.readLine()) != null) {
			lines.add(str);
		}
		br.close();
		isr.close();
		dis.close();
		System.out.println("Original file reading complete.");
		return lines;
	}

	// 获取文件路径
	public static String getLocation(String path) throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		Path listf = new Path(path);
		FileStatus stats[] = hdfs.listStatus(listf);
		String FilePath = stats[0].getPath().toString();
		/*
		 * for(int i = 0; i < stats.length; ++i){
		 * System.out.println(stats[i].getPath().toString()); }
		 */
		hdfs.close();
		System.out.println("Find input file.");
		return FilePath;
	}

	// 删除文件和文件夹
	public static void deleteFile(String fileName) throws IOException {
		Path f = new Path(fileName);
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(URI.create(fileName), conf);
		boolean isExists = fs.exists(f);
		if (isExists) { // if exists, delete
			boolean isDel = fs.delete(f, true);
			System.out.println(fileName + "  delete? \t" + isDel);
		} else {
			System.out.println(fileName + "  exist? \t" + isExists);
		}
	}

	// pre processing
	public static void preprocessingphase1(String[] args) throws Exception {
		// making the temp input files.
		// Do we need this step?
		String originalFilePath = getLocation(args[0]);
		System.out.println(originalFilePath);
		if (originalFilePath == null)
			return;
		List<String> lines = readFile(originalFilePath);
		if (lines == null)
			return;
		totalLineNum = lines.size();
		subfileNum = Integer.parseInt(args[1]);
		int lineNumPerFile = (int) Math.ceil((1.0 * totalLineNum) / subfileNum);
		mkdir("input_temp");
		for (int currSubfile = 0; currSubfile < subfileNum; currSubfile++) {
			String subfilePath = "input_temp/" + currSubfile + ".dat";
			String subfileContents = "";
			for (int lineIdx = 0; lineIdx < lineNumPerFile
					&& lineIdx < totalLineNum - currSubfile * lineNumPerFile; lineIdx++) {
				subfileContents += lines.get(currSubfile * lineNumPerFile
						+ lineIdx)
						+ "\n";
			}
			createFile(subfilePath, subfileContents.getBytes());
			// appendToFile(newFile, input_temp);
		}
	}

	// phase 1
	public static void phase1(String[] args) throws Exception {
		// What's args[2]?
		supportThreshold = Double.parseDouble(args[2]);
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Finding Frequent Itemsets");
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
	

	// pre processing for phase 2
	public static void preprocessingphase2(String[] args) throws Exception {
		List<String> lines = readFile("output_temp/part-r-00000");
		Iterator<String> itr = lines.iterator();
		while (itr.hasNext()) {
			String basket = (String) itr.next();
			String[] items = basket.split(" |\t");
			List<String> temp = new ArrayList<String>();
			Collections.addAll(temp, items);
			int n = temp.size();
			temp.remove(n - 1);
			firstphaseset.add(temp);
		}
		//
		// System.out.println(firstphaseset);
		System.out.println("Pre processing for phase 2 finished.");
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
		FileInputFormat.setInputPaths(job, new Path("input_temp"));
		FileOutputFormat.setOutputPath(job, new Path("output"));
		job.waitForCompletion(true);
	}

	// the merge sort procedure
	public static void merge_sort() {
		int l = 0;
		int r = secondphaseset.size() - 1;
		int len = r - l + 1;
		// 枚举的是一半的长度
		for (int i = 1; i <= len; i *= 2) {
			int left = l;
			while (left < r) {
				int mid = left + i - 1;
				int right = left + 2 * i - 1;
				// 中间值大于右边界，说明排好序了
				if (mid > r)
					break;
				// 中间值没有超，右边界超了
				if (right > r)
					right = r;
				// mid和right相等的时候，也不需要排序
				if (right == mid)
					break;
				List temp = new ArrayList();
				merge(left, mid, right, temp);
				left = right + 1;
			}
		}
	}

	public static boolean smaller(List a, List b) {
		int aa = Integer.parseInt((String) a.get(1));
		int bb = Integer.parseInt((String) b.get(1));
		int value = 0;
		if (aa < bb)
			value = -1;
		if (aa > bb)
			value = 1;
		if (aa == bb) {
			String aaa = (String) a.get(0);
			String bbb = (String) b.get(0);
			value = -aaa.compareTo(bbb);
		}
		if (value < 0)
			return false;
		else
			return true;
	}

	public static void merge(int left, int middle, int right, List temp) {
		int i = left, j = middle + 1;
		int m = middle, n = right;
		int k = 0;
		while (i <= m && j <= n) {
			if (smaller((List) secondphaseset.get(i),
					(List) secondphaseset.get(j))) {
				temp.add(secondphaseset.get(i));
				i++;
			} else {
				temp.add(secondphaseset.get(j));
				j++;
			}
		}
		while (i <= m) {
			temp.add(secondphaseset.get(i));
			i++;
		}
		while (j <= n) {
			temp.add(secondphaseset.get(j));
			j++;
		}
		k = temp.size();
		for (int v = 0; v < k; v++)
			secondphaseset.set(left + v, temp.get(v));
	}

	// final process
	public static void finalprocess() throws Exception {
		deleteFile("output_temp");
		deleteFile("input_temp");
		secondphaseset.clear();
		List<String> lines = readFile("output/part-r-00000");
		for (Iterator i = lines.iterator(); i.hasNext();) {
			String str = (String) i.next();
			List temp = Arrays.asList(str.split("\t"));
			secondphaseset.add(temp);
		}
		List temp = new ArrayList();
		merge_sort();
		@SuppressWarnings("unused")
		String str_finial = "";
		for (Iterator i = secondphaseset.iterator(); i.hasNext();) {
			List tmp = (List) i.next();
			String str = tmp.get(0) + " (" + tmp.get(1) + ")\n";
			str_finial += str;
		}
		mkdir("result");
		String str = "" + secondphaseset.size() + "\n";
		createFile("result/result.txt", str.getBytes());
		appendToFile("result/result.txt", str_finial);
		System.out.println("All finished.");
	}

	// main function
	public static void main(String[] args) throws Exception {
		if (args.length < 3) {
			System.out.println("The number of arguments is less than three.");
			return;
		}
		// first phase
		preprocessingphase1(args);
		phase1(args);

		// second phase
		preprocessingphase2(args);
		phase2(args);

		// final process
		finalprocess();
>>>>>>> fb31f0088fc4333a441c9239980889dd8bbb5872:src/FrequentItemset.java
	}
}
