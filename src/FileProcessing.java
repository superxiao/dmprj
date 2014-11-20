import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class FileProcessing {
		//创建目录
		public static void mkdir(String path) throws IOException{
	        Configuration conf = new Configuration();
	        FileSystem fs = FileSystem.get(conf);
	        Path srcPath = new Path(path);
	        boolean isok = fs.mkdirs(srcPath);
	        if(isok){
	            System.out.println("create dir ok.");
	        }else{
	            System.out.println("create dir failure.");
	        }
	        fs.close();
	    }
		//创建新文件
		public static void createFile(String dst , byte[] contents) throws IOException{
	        Configuration conf = new Configuration();
	        FileSystem fs = FileSystem.get(conf);
	        Path dstPath = new Path(dst); //目标路径
	        //打开一个输出流
	        FSDataOutputStream outputStream = fs.create(dstPath);	
	        outputStream.write(contents);
	        outputStream.close();
	        fs.close();
	        System.out.println("file "+dst+" create complete.");
	    }
		//追加写入文件
		public static void appendToFile(String dst, String line) throws FileNotFoundException,IOException {
			  Configuration conf = new Configuration();  
			  FileSystem fs = FileSystem.get(conf); 
			  FSDataOutputStream outputStream = fs.append(new Path(dst));
			  outputStream.write(line.getBytes());
			  outputStream.close();
			  fs.close();
			 }
		//读取文件的内容
	    public static List<String> readFile(String filePath) throws IOException{
	    	Path f = new Path(filePath);
	    	Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(URI.create(filePath), conf);
	    	FSDataInputStream dis = fs.open(f);
	    	InputStreamReader isr = new InputStreamReader(dis, "utf-8");
	    	BufferedReader br = new BufferedReader(isr);
	    	List<String> lines = new ArrayList<String>();
	    	String str = "";
	    	while((str = br.readLine()) !=null){
	    			lines.add(str);
	    	}
	    	br.close();
	    	isr.close();
	    	dis.close();
	    	System.out.println("Original file reading complete.");
	    	return lines;
	    }
	    public static byte[] readLocalFile(String path) throws IOException{
	    	//BufferedReader br = new BufferedReader(new FileReader(path)); 
	    	//List <String> lines = new ArrayList<String>();
	    	File file = new File(path);
	    	FileInputStream fis = new FileInputStream(file);
	    	byte[] data = new byte[(int)file.length()];
	    	fis.read(data);
	        fis.close();
	        return data;
	    	//return ;
	    }
	    //获取文件路径
	    public static String getLocation(String path) throws Exception {
	        Configuration conf=new Configuration();
	        FileSystem hdfs=FileSystem.get(conf);
	        Path listf =new Path(path);
	        FileSystem fs = FileSystem.get(URI.create(path), conf);
	        boolean isExists = fs.exists(listf);
	        if (!isExists) return null;
	        FileStatus stats[]=hdfs.listStatus(listf);
	        String FilePath = stats[0].getPath().toString();
	        /*for(int i = 0; i < stats.length; ++i){
	        	System.out.println(stats[i].getPath().toString());
	        	}*/
	        hdfs.close();
	        System.out.println("Find input file.");
	        return FilePath;
	    }
	    //删除文件和文件夹
	    public static void deleteFile(String fileName) throws IOException {
	        Path f = new Path(fileName);
	        Configuration conf = new Configuration();
	        FileSystem fs = FileSystem.get(URI.create(fileName), conf);
	        boolean isExists = fs.exists(f);
	        if (isExists) { //if exists, delete
	            boolean isDel = fs.delete(f,true);
	            System.out.println(fileName + "  delete? \t" + isDel);
	        } else {
	            System.out.println(fileName + "  exist? \t" + isExists);
	        }
	    }
	    public static void upload(String path) throws IOException{
	    	/*List localdata = readLocalFile(path);
	    	String data = "";
	    	int n = localdata.size();
	    	for (int i = 0; i < n; i ++){
	    		data += localdata.get(i)+"\n";
	    	}*/
	    	byte [] data = readLocalFile(path);
	    	mkdir("input");
	    	createFile("input/data.txt", data);
	    }
	    public static void clean() throws IOException {
	    	deleteFile("input_temp");
	    	deleteFile("output_temp");
	    	deleteFile("input");
	    	deleteFile("output");
	    	deleteFile("result");
	    }
}
