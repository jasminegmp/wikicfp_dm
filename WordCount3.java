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

// WordCount3 - For each conference, regardless of year output the list of cities
// Builds off of original WordCount.java provided by Hadoop
// https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html


public class WordCount3 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{
	// Very similar mapper as what I did in WordCount2.java
	// Difference is that I strip off the year in the conference acronym
	// Example: KDD 2016 is turned into KDD
	// This is so I can have conference acronym keys match
    private final static IntWritable one = new IntWritable(1);
    private Text conf_acro = new Text();
    private Text conf_name = new Text();
    private Text conf_loc = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
      while (itr.hasMoreTokens()) {
	  	// Here is where I strip off the year
		conf_acro.set(itr.nextToken().split("\\s+")[0]);
		conf_name.set(itr.nextToken());
		conf_loc.set(itr.nextToken());
		context.write(conf_acro, new Text(conf_loc));
      }
    }
  }

  public static class IntSumReducer
   	// In my reducer, I want to reduce on the conference (conf_acro)
	// And keep appending the value (location) onto matching keys
	// Example <KDD, Los Angeles> and <KDD, Tokyo> 
	// Would reduce down to <KDD, Los Angeles Tokyo>
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      String temp = "";    
      String prev_loc = "";
      String curr_loc = "";
      for (Text val : values) {
		curr_loc = val.toString();
		if (!curr_loc.equals(prev_loc))
			{
			temp += val.toString();
			prev_loc = val.toString();
			temp += " ";
		}
        
      }
      result.set(temp);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount3.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
