
题目2：编写MapReduce，统计`/user/hadoop/mapred_dev/ip_time` 中去重后的IP数，越节省性能越好。（35分）
package cou;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MRBench.Map;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class ipcunt extends Mapper<LongWritable, Text, Text, Text> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{   
		String line = value.toString();
		 		String [] a = line.split("\t");
		 		String ip = a[0];
		 		context.write(new Text(ip), new Text(""));
          
         
        };  
        public static class Reduce extends Reducer<Text,Text,Text,Text>{        	 
        	 		public void reduce(Text key,Iterable<Text> values,Context context) throws IOException, InterruptedException{        	 		
        	 			context.write(key, new Text(""));
        	 		}
        }
        public static void main(String[] args)
        	throws IOException, ClassNotFoundException, InterruptedException{
        	JobConf conf = new JobConf(MapReduce.class);
        	conf.setJobName("WordCount");
        	conf.setOutputKeyClass(Text.class);
        	conf.setOutputValueClass(IntWritable.class);

        	conf.setMapperClass(Map.class);
        	conf.setReducerClass(Reduce.class);
        	conf.setNumReduceTasks(2);
        	conf.setInputFormat(TextInputFormat.class);
        	conf.setOutputFormat(TextOutputFormat.class);

        	FileInputFormat.setInputPaths(conf, new Path(args[0]));
        	FileOutputFormat.setOutputPath(conf, new Path(args[1]));

        	JobClient.runJob(conf);
        	}  
        		 		    	
        }

运行完之后，描述程序里所做的优化点，每点+5分。
