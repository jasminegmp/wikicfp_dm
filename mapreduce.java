import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Scanner;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class WordCount2 {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    private final static IntWritable one = new IntWritable(1);
    private Text conf_acro = new Text();
    private Text conf_name = new Text();
    private Text conf_loc = new Text();

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString(), "\t");
    //String[] itr = value.toString().split("\t");
    //conf_acro.set(itr[0]);
    //conf_name.set(itr[1]);
    //conf_name.set(itr[2]);
      while (itr.hasMoreTokens()) {
    
    conf_acro.set(itr.nextToken());
    
    conf_name.set(itr.nextToken());
    conf_loc.set(itr.nextToken());
    context.write(conf_loc, new Text(conf_acro));
    //System.out.println(conf_name);
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
      //int sum = 0;
      String temp = "";
      /*
      while(values.hasNext()){
      temp = temp + values.next().toString();
    }*/
      
      for (Text val : values) {
        temp += val.toString();
        
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
    //job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
