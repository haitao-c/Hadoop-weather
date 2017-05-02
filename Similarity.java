import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;  
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Similarity{	
	private static List<String> list = new ArrayList<String>();
	private static Set<String> set = new HashSet<String>();
	public static class IdMapper extends Mapper<LongWritable, Text, Text, Text>{
		private Text id = new Text();
		private Text yearTemp = new Text();
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			String[] array = value.toString().split("\t");					
			id.set(array[0]);
			yearTemp.set(array[1]+array[2]);
			context.write(id,yearTemp);
		}
	}
	

	public static class YearTempReducer extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();
		//private Text idid = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			StringBuilder stringBuilder = new StringBuilder();
			for(Text val:values){
				stringBuilder.append(val.toString()+",");
			}
			stringBuilder.deleteCharAt(stringBuilder.length()-1);
			result.set(stringBuilder.toString());
			context.write(key,result);	
			if(!set.contains(key.toString()))
				set.add(key.toString());
		}
	}

	public static class Job2_Mapper extends Mapper<LongWritable, Text, Text, Text>{
		private Text idid = new Text();	
		private Text result = new Text();
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			String[] array = value.toString().split("\t");
			String currentId = array[0];
			String ididString = null;
			boolean flag = false;
			for(String val:list){
				if(!flag){
					if(!currentId.equals(val)){
						ididString = val+"\t"+currentId;						
					}else{
						flag = true;
						continue;
					}
				}else{
					ididString = currentId+"\t"+val;
				}
				idid.set(ididString);
				result.set(array[1]);
				context.write(idid,result);
			}
		}	

	}

	public static class Job2_Reducer extends Reducer<Text,Text,Text,FloatWritable> {
		private List<String> yearTemperatureList = new ArrayList<String>();
		private  FloatWritable similarityScore = new FloatWritable(); 
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			int count = 0;
			String[] yearTemperatureArray=null;
			boolean flag = false;
			float similarityScoreFloat = 0f;
			for(Text val:values){
				String[] yearTemperatureTemp = val.toString().split(",");
				for(String yearTemperature:yearTemperatureTemp)
					yearTemperatureList.add(yearTemperature);
			}
			yearTemperatureArray = (String[])yearTemperatureList.toArray(new String[yearTemperatureList.size()]);
			for(int i=0;i<yearTemperatureArray.length;i++){
				if(yearTemperatureArray[i]!=null){
					String year = yearTemperatureArray[i].substring(0,4);
					int temperature = Integer.parseInt(yearTemperatureArray[i].substring(4));
					for(int j=i+1;j<yearTemperatureArray.length;j++){
						if(yearTemperatureArray[j]!=null){
							String latterYear = yearTemperatureArray[j].substring(0,4);
							int latterTemperature = Integer.parseInt(yearTemperatureArray[j].substring(4));
							if(year.equals(latterYear)){
								sum+=Math.abs(temperature-latterTemperature);
								count++;
								yearTemperatureArray[i]=null;
								yearTemperatureArray[j]=null;
								flag = true;
							}
						}						
					}
					if(!flag){
						sum+=Math.abs(temperature-0);
						count++;
					}
				}				
			}
			similarityScoreFloat = sum/count;
			similarityScore.set(similarityScoreFloat);
			context.write(key,similarityScore);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job1 = Job.getInstance(conf);
		job1.setJarByClass(Similarity.class);
		job1.setMapperClass(IdMapper.class);
    //job.setCombinerClass(FloatAveReducer.class);
		job1.setReducerClass(YearTempReducer.class);
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1])); 		

		if(job1.waitForCompletion(true)){
			for(String val:set)
				list.add(val);
			Collections.sort(list);
			Job job2 = new Job(conf);
			FileInputFormat.addInputPath(job2, new Path(args[1]));
			job2.setMapperClass(Job2_Mapper.class);  
			job2.setReducerClass(Job2_Reducer.class);  
			FileOutputFormat.setOutputPath(job2, new Path(args[2]));  
			job2.setOutputKeyClass(Text.class);  
			job2.setOutputValueClass(Text.class);  			
			System.exit(job2.waitForCompletion(true) ? 0 : 1);


		}    

	}

}