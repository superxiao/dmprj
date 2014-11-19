import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.Collections;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;

public class FindingFrequentItemset {
	static double supportThreshold = 0.0;
	static int totalLineNum = 0;
	static int subfileNum = 1;
	static String SET = "set";
	static String INDEX = "index";
	@SuppressWarnings("rawtypes")
	static List<List> firstphaseset = new ArrayList<List>();
	static List secondphaseset = new ArrayList();
	static String [] myargs = null;
	
	// Mapper class for the first phase MapReduce.
	public static class FirstMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
		static Set set_temp = new HashSet();
		//using backtracking algorithm to find all the itemsets in one basket
		public  static void generateitemset_recursive(String[] basket, int p,List temp){
			for (int i = p; i < basket.length; i++){
				temp.add(basket[i]);
				set_temp.add(new ArrayList(temp));
				generateitemset_recursive(basket, i+1, temp);
				temp.remove(basket[i]);
			}
		}
		public static void generateitemset_nonrecursive(String[] basket){
			int p = 0;
			Stack<Map>s = new Stack<Map>();
			s.clear();
			Map subset_dictionary = new HashMap();
			List<String> subset_temp = new ArrayList<String>();
			subset_temp.add(basket[p]);
			set_temp.add(subset_temp);
			subset_dictionary.put(SET, subset_temp);
			subset_dictionary.put(INDEX, p+1);
			s.add(subset_dictionary);
			while(!s.isEmpty()){
				Map temp = new HashMap(s.peek());
				int n = (Integer) temp.get(INDEX);
				if (n < basket.length){
					subset_temp = new ArrayList<String>((List) temp.get(SET));
					subset_temp.add(basket[n]);
					set_temp.add(subset_temp);
					temp.put(SET, subset_temp);
					temp.put(INDEX, n+1);
					s.push(temp);
				}else{
					temp = new HashMap(s.peek());
					s.pop();
					 if (!s.isEmpty()){
						 temp = new HashMap(s.peek());
						 s.pop();
						 int index = (Integer) temp.get(INDEX);
						 index += 1;
						 temp.put(INDEX, index);
						 subset_temp = new ArrayList<String>((List) temp.get(SET));
						 int index2 = subset_temp.size();
						 subset_temp.remove(index2-1);
						 subset_temp.add(basket[index-1]);
						 set_temp.add(subset_temp);
						 temp.put(SET, subset_temp);
						 s.add(temp);
					 }
				}
			}
		}
		
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) 
			throws IOException {
			//split all the baskets in the subfile
			String data = value.toString();
			String[] baskets = data.split("\n");
			int n = baskets.length;
			//use dictionary to memory the count of <key, value>
			@SuppressWarnings("rawtypes")
			Map<List, Integer> itemset_dictionary = new HashMap<List, Integer>();
			for (int i = 0; i < n; i++){
				//generate all the itemsets in a basket
				set_temp.clear();
				List<String> l = new ArrayList<String>();
				//recursive function to find all the subsets for a baskets
				//generateitemset_recursive(baskets[i].split(" "), 0, l);
				//non recursive function to find all the subsets for a basket
				generateitemset_nonrecursive(baskets[i].split(" "));
				@SuppressWarnings("rawtypes")
				Iterator<List> it = set_temp.iterator();
				while (it.hasNext()){
					List list_temp = it.next();
					if (itemset_dictionary.containsKey(list_temp)){
						Integer val = itemset_dictionary.get(list_temp)+1;
						itemset_dictionary.put(list_temp, val);
					}else{
						itemset_dictionary.put(list_temp, 1);
					}
				}
			}
			
			//output the frequent itemsets in one subfile
			IntWritable one = new IntWritable(1);
			for (List key_temp: itemset_dictionary.keySet()){
				int count = (int)itemset_dictionary.get(key_temp);
				if (count >=supportThreshold*n){
					String str_temp = "";
					for (int i = 0; i < key_temp.size(); i++){
						if (i == 0) str_temp += key_temp.get(i);
						else str_temp += " "+key_temp.get(i);
					}
					Text word = new Text(str_temp);
					output.collect(word, one);
					//output.collect(word, new IntWritable(count));
				}
			}
		}
	}
	
	public static class FirstReduce extends MapReduceBase implements
    Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterator <IntWritable> values, OutputCollector <Text, IntWritable> output, Reporter reporter)
			throws IOException{
				int sum = 0;
				while(values.hasNext()){
					sum += values.next().get();
					//values.next().get();
				}
				//output.collect(key, new IntWritable(sum));
				output.collect(key, new IntWritable(1));
		}
	}
	//the second phase
	public static class SecondMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String data = value.toString();
			String[] baskets = data.split("\n");
			int n = baskets.length;
			List<String> basket_set = new ArrayList<String>();
			Map<List, Integer> frequent_itemset_count = new HashMap<List, Integer>();
			//using two loops to count the frequent of every candidate subset.
			for (int i = 0; i < n; i++){
				String[] basket = baskets[i].split(" ");
				int m = basket.length;
				basket_set.clear();
				for (int j = 0; j < m; j++) basket_set.add(basket[j]);
				m = firstphaseset.size();
				for (Iterator j = firstphaseset.iterator(); j.hasNext();){
					List s = new ArrayList((List)j.next());
					if (basket_set.containsAll(s)){
						if (frequent_itemset_count.containsKey(s)){
							Integer v = frequent_itemset_count.get(s);
							v += 1;
							frequent_itemset_count.put(s, v);
						}else{
							frequent_itemset_count.put(s, new Integer(1));
						}
					}
				}
			}
			for (List candidate: frequent_itemset_count.keySet()){
				String str_temp = "";
				int m = candidate.size();
				int count = frequent_itemset_count.get(candidate);
				for (int i = 0; i < m; i++){
					if (i == 0) str_temp += candidate.get(i);
					else str_temp += " "+candidate.get(i);
				}
				Text set = new Text(str_temp);
				output.collect(set, new IntWritable(count));
			}
		}
	}
	public static class Second_Reduce extends MapReduceBase implements
    Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterator <IntWritable> values, OutputCollector <Text, IntWritable> output, Reporter reporter)
				throws IOException{
			int sum = 0;
			while(values.hasNext()){
				sum += values.next().get();
			}
			if (sum >= supportThreshold*totalLineNum) output.collect(key, new IntWritable(sum));
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
	}
}
