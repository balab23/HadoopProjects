import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.util.*;


import java.util.regex.*;



public class regularhadoop {

	public static class ipmap extends Mapper<LongWritable, Text, Text, IntWritable> 
	{
	  private final static IntWritable one =new IntWritable(1);
	  private Text word=new Text();
	  
	  public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
	  {
		 String line=value.toString();
		 StringTokenizer is=null ;
		 
		// System.out.println(line);
		 //System.out.println("yes");
		 //170.210.192.3
		 
		 
		 
		 Pattern checkregex=Pattern.compile("(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)(\\.(25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)){3}");
	     
		 Matcher match=checkregex.matcher(line);
	     
		 
	     
	     if(match.find()) {
	    	 
	    	 System.out.println(match.group());
	    	 is=new StringTokenizer(match.group());
			      
	     }
	     
	     else is=new StringTokenizer(line);
	     
	     
		 
	     
	     while(is.hasMoreTokens())
	    	 {
	    	     
	    	    
		     
	             word.set(is.nextToken());
	    	     context.write(word, one);
	    	   
	    	 }
	    }
	     
	     
	  }
	
      public static class ipreduce extends Reducer<Text, IntWritable, Text,IntWritable>
      {
    	  public void reduce(Text key, Iterable<IntWritable>values, Context context)
    			  throws IOException, InterruptedException
    	  
    	  {
    		int sum=0;
    		
    		for(IntWritable val:values)
    		{
    			sum=sum+val.get();
    		}
    		
    		context.write(key,new IntWritable(sum));
    	  }
      }
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
	   
	   public static void main(String[] args) throws Exception{
		   
	   Configuration conf=new Configuration();
	   
	   Job job= new Job(conf,"ipcount");
	   
	   job.setOutputKeyClass(Text.class);
	   job.setOutputValueClass(IntWritable.class);
	   
	   job.setMapperClass(ipmap.class);
	   job.setReducerClass(ipreduce.class);
	   
	   job.setInputFormatClass(TextInputFormat.class);
	   job.setOutputFormatClass(TextOutputFormat.class);
	   
	   FileInputFormat.addInputPath(job, new Path ("C:\\Users\\balaji\\Desktop\\LOG\\cnameRRs.dns"));
	   FileOutputFormat.setOutputPath(job, new Path ("C:\\Users\\balaji\\Desktop\\LOGOUT"));
	   
	  job.waitForCompletion(true); 
	   }

}

