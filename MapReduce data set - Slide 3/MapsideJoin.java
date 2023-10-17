import java.io.IOException;
import java.io.*;
import java.util.*;
import java.net.URI;
  
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
 
 public class MapsideJoin {
 

 public static class TransMapper extends Mapper <Object, Text, Text, Text>
 {
	 // user map to keep the userId-userName
	 private Map<Integer, String> userMap = new HashMap<>();
	 
public void setup(Context context) throws IOException,
		InterruptedException
{	
	  try (BufferedReader br = new BufferedReader(new FileReader("cust"))) {
		   String line;
		   while ((line = br.readLine()) != null) {
		    String columns[] = line.split(",");
			//String id = 
			//String name = 
		    userMap.put(Integer.parseInt(id), name);
		   }
		  } catch (IOException e) {
		   e.printStackTrace();
		  }
}


 public void map(Object key, Text value, Context context) 
 throws IOException, InterruptedException 
 {
		//write your code here
 }
 }
 

 
 public static void main(String[] args) throws Exception {
 Configuration conf = new Configuration();
 Job job = new Job(conf, "Mapside Join");
 job.setJarByClass(MapsideJoin.class);
 job.setMapperClass(TransMapper.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(Text.class);
 // Setting reducer to zero
 job.setNumReduceTasks(0);
  
 
 try {
	  
     job.addCacheFile(new URI("hdfs://localhost:8020/mycache/cust"));
 }
 catch (Exception e) {
     System.out.println("File Not Added");
     System.exit(1);
 }
 
 FileInputFormat.addInputPath(job, new Path(args[0]));
 FileOutputFormat.setOutputPath(job, new Path(args[1]));
 System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
 }


