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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


public class WordCount {

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text likeWord = new Text();
    private Text retweetWord = new Text();
    final String DELIMITER = ",";
   

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                   
      String[] tokens = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
      if(tokens[1].toString().equals("tweet_id"))
        return;
       
         
      String tweetStr = tokens[2];
      //like
      String likeStr = tokens[3];
      //retweet
      String retweetStr = tokens[4];

      boolean haveTrump = false;
      boolean haveBiden = false;

      //Check for the see tweet have trump or biden
      if(tweetStr.indexOf("#DonaldTrump") != -1 || tweetStr.indexOf("#Trump") != -1)
	haveTrump = true;

      if(tweetStr.indexOf("#Biden") != -1 || tweetStr.indexOf("#JoeBiden") != -1)
	haveBiden = true;

      IntWritable likeNUM = new IntWritable((int)Float.parseFloat(likeStr));
      IntWritable tweetNUM = new IntWritable((int)Float.parseFloat(retweetStr));

      if(haveTrump && !haveBiden)
      {
	likeWord.set("Trump likes: ");
        retweetWord.set("Trump Retweets: ");
	}
      else if(!haveTrump && haveBiden)
      {
	likeWord.set("Biden likes: ");
        retweetWord.set("Biden Retweets: ");
       }
      else if(haveTrump && haveBiden)
      {
	likeWord.set("Both likes: ");
        retweetWord.set("Both Retweets: ");
       }

      context.write(likeWord, likeNUM);
      context.write(retweetWord, tweetNUM);
   
 
    }
  }
 

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    //job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
