import java.io.IOException;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import regularhadoop.ipmap;
import regularhadoop.ipreduce;
        


public class BOOK {

	public static class wordmap extends Mapper<LongWritable, Text, Text, Text>
	
	{
		private Text word= new Text();
		private Text bnm=new Text();
		 
		public void map(Text key, Text value, Context context) throws IOException, InterruptedException
		{
		    String line=value.toString();
		    StringTokenizer to= null;
		    StringTokenizer is=null;
		    
		    Pattern bookname=Pattern.compile("(\\[\"\\w{1,20}\\-\\w{1,20}\\.\\w{1,4}\")");
	        Matcher bname=bookname.matcher(line);	
	        
	        Pattern bookcontent=Pattern.compile("(\\]\\s.+\\.\"\\])");
	        Matcher bcontent=bookcontent.matcher(line);	
		
	        if(bname.find())
	        {
	        	System.out.println(bname.group());
	        	to=new StringTokenizer(bname.group());
	        }
	        
	        if(bcontent.find())
	        {
	        	System.out.println(bcontent.group());
	        	is=new StringTokenizer(bcontent.group());
	        }
	        
		    while(to.hasMoreTokens() && is.hasMoreTokens())
		    {
		    	bnm.set(to.nextToken());
		    	
		    	while(is.nextToken()!=".\"]")
		    	{
		    	word.set(is.nextToken());
		    	context.write(word,bnm);
		    	}
		        
		    	is.nextToken();
		    }
		
		}
	}
	
	public static class wordreduce extends Reducer<Text,Text,Text,Text>
	{
		public void reduce(Text key, Iterable<Text>values, Context context)throws IOException, InterruptedException 
		{
			
			StringBuilder builder = new StringBuilder();
			
			for(Text val:values)
			{
				
				String str=val.toString();
				builder.append(str);
			}
		    String out=builder.toString();
		    
		    context.write(key,new Text(out));
		}
	
	}
	
	public static void main(String[] args) {
		
		 Configuration conf=new Configuration();
		   
		   Job job= new Job(conf,"book");
		   
		   job.setOutputKeyClass(Text.class);
		   job.setOutputValueClass(Text.class);
		   
		   job.setMapperClass(wordmap.class);
		   job.setReducerClass(wordreduce.class);
		   
		   job.setInputFormatClass(TextInputFormat.class);
		   job.setOutputFormatClass(TextOutputFormat.class);
		   
		   FileInputFormat.addInputPath(job, new Path ("C:\\Users\\balaji\\Downloads\\books.json"));
		   FileOutputFormat.setOutputPath(job, new Path ("C:\\Users\\balaji\\Desktop\\BOOKOUT"));
		   
		  job.waitForCompletion(true); 

	}

}
