import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


// WordCount2 - Output list of conferences per city
// Builds off of original WordCount.java provided by Hadoop
// https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html

public class WordCount2 {

  public static class TokenizerMapper
	   // In my mapper, want to output <conf_loc, conf_acro> ex: <KDD 2016, Tokyo> 
	   // Want output key and value to  be type Text
       extends Mapper<Object, Text, Text, Text>{

    private final static IntWritable one = new IntWritable(1);
    private Text conf_acro = new Text();
    private Text conf_name = new Text();
    private Text conf_loc = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
      while (itr.hasMoreTokens()) {
	  	// Store off each token (3) per line
		conf_acro.set(itr.nextToken());
		conf_name.set(itr.nextToken());
		conf_loc.set(itr.nextToken());
		// Write key and value output as text with location and acronym
		context.write(conf_loc, new Text(conf_acro));
      }
    }
  }

  public static class IntSumReducer
  	// In my reducer, I want to reduce on the key (location)
	// And keep appending the value (conf_acro) onto matching keys
	// Example <Los Angeles, KDD 2016> and <Los Angeles, ICDM 2017> 
	// Would reduce down to <Los Angeles, KDD 2016 ICDM 2017>
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String temp = "";
      for (Text val : values) {
        temp += val.toString();
        temp += " ";
      }
      result.set(temp);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount2.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
	// New output value type is Text
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
