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
  // We give the Object and Text(This is csv file text) and return Text(america and france) and Text (X|X|X|X)
  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, Text>{

    //The Country name
    private Text countryName = new Text();
    //The X|X|X|X format of data needed
    private Text dataHadoop = new Text();
   

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                   
      String[] tokens = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
      if(tokens[1].toString().equals("tweet_id"))
        return;
       
         
      String tweetStr = tokens[2];
      //lat of location
      String latStr = tokens[13];
      //long of location
      String longStr = tokens[14];
      
      if(latStr.equals("") || longStr.equals(""))
      	return;
     
      Float latF = Float.parseFloat(latStr);
      Float longF = Float.parseFloat(longStr);
     
      if(longF > -161.75 && longF < -68.00 && latF > 19.5 && latF < 64.85)
      	countryName.set("america");
     
     
      if(longF > -4.65 && longF < 9.45 && latF > 41.6 && latF < 51)
      	countryName.set("france");    
     
         
      if(countryName.toString().equals(""))
        return;
       
      boolean haveTrump = false;
      boolean haveBiden = false;

      //Check for the see tweet have trump or biden
      if(tweetStr.indexOf("#DonaldTrump") != -1 || tweetStr.indexOf("#Trump") != -1)
	haveTrump = true;

      if(tweetStr.indexOf("#Biden") != -1 || tweetStr.indexOf("#JoeBiden") != -1)
	haveBiden = true;


      //If the tweet Have Trump and not Biden	 
      if(haveTrump && !haveBiden)
      {
	dataHadoop.set("0|0|1|1");
	}
      //If the tweet Have Boden and not Trump
      else if(!haveTrump && haveBiden)
      {
	dataHadoop.set("0|1|0|1");
       }
      //If the tweet Have Trump and Biden
      else if(haveTrump && haveBiden)
      {
	dataHadoop.set("1|0|0|1");
      }
      //If the tweet Have not Trump and not Biden
      else
      {
	dataHadoop.set("0|0|0|1");
      }


      context.write(countryName, dataHadoop);
    }
  }
 

  public static class IntSumReducer
       extends Reducer<Text,Text,Text,Text> {
    private Text result = new Text();

    public void reduce(Text key, Iterable<Text> values,
                       Context context
                       ) throws IOException, InterruptedException {
                       
      //This is the variable to 
      Double both = new Double(0);
      Double biden = new Double(0);
      Double trump = new Double(0);
      Double all = new Double(0);
     
      for (Text val : values) {
        if(val.toString().charAt(0) == '1')
        	both++;
        if(val.toString().charAt(2) == '1')
        	biden++;
        if(val.toString().charAt(4) == '1')
        	trump++;
        if(val.toString().charAt(6) == '1')
        	all++;
      }
      both = both / all;
      biden = biden / all;
      trump = trump / all;
      Integer Tall = all.intValue();
     
      result.set(both.toString() + " " + biden.toString() +  " " + trump.toString() +  " " + Tall.toString());
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
