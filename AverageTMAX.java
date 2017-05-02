import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class AverageTMAX{
	static final String TMAX="TMAX";
	public static class DaillyTempMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		private Text id = new Text();
		private IntWritable val = new IntWritable();
		public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
			String[] array = value.toString().split(",");					
			if(TMAX.equals(array[2])){				
				String idTemp = array[0]+array[1].substring(0,4);
				int temperature = Integer.parseInt(array[3]);
				//System.out.println(yearlyTemp);
				id.set(idTemp);
				val.set(temperature);
				context.write(id,val);
			}
		}
	}

	public static class FloatAveReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int sum = 0;
			int count = 0;
			float average = 0f;
			String idYear = key.toString();
			Text idText = new Text();
			IntWritable result = new IntWritable();
			for(IntWritable val:values){				
				sum += val.get();
				count++;				
			}
			average = sum/count;
			Float temp = new Float(average);
			int out = temp.intValue();
			String idString = idYear.substring(0,11)+"\t"+idYear.substring(11);			
			idText.set(idString);
			result.set(out);
			context.write(idText,result);			
		}
	}
	public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "averate TMAX");
    job.setJarByClass(AverageTMAX.class);
    job.setMapperClass(DaillyTempMapper.class);
    //job.setCombinerClass(FloatAveReducer.class);
    job.setReducerClass(FloatAveReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }

}