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
       extends Mapper<Object, Text, Text, Text>{


    private Text countryName = new Text();
    private Text dataHadoop = new Text();
   

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
                   
      String[] tokens = value.toString().split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
      if(tokens[1].toString().equals("tweet_id"))
        return;
       
         
      String tweetStr = tokens[2];
      String countryStr = tokens[16].toLowerCase();
     
      if(countryStr.indexOf("america") != -1)
      	countryName.set("america");
      if(countryStr.indexOf("iran") != -1)
      	countryName.set("iran");
      if(countryStr.indexOf("netherlands") != -1)
      	countryName.set("netherlands");
      if(countryStr.indexOf("australia") != -1)
      	countryName.set("australia");
      if(countryStr.indexOf("mexico") != -1)
      	countryName.set("mexico");    
      if(countryStr.indexOf("emirates") != -1)
      	countryName.set("emirates");      
      if(countryStr.indexOf("france") != -1)
      	countryName.set("france");    
      if(countryStr.indexOf("germany") != -1)
      	countryName.set("germany");    
      if(countryStr.indexOf("england") != -1 || countryStr.indexOf("kingdom") != -1)
      	countryName.set("engalnd");    
      if(countryStr.indexOf("canada") != -1)
      	countryName.set("canada");    
      if(countryStr.indexOf("spain") != -1)
      	countryName.set("spain");    
      if(countryStr.indexOf("italy") != -1)
      	countryName.set("italy");    
             
         
      if(countryStr.equals("") || countryName.toString().equals(""))
        return;
       
      boolean haveTrump = false;
      boolean haveBiden = false;


      //Check for the see tweet have trump or biden
      if(tweetStr.indexOf("#DonaldTrump") != -1 || tweetStr.indexOf("#Trump") != -1)
	haveTrump = true;

      if(tweetStr.indexOf("#Biden") != -1 || tweetStr.indexOf("#JoeBiden") != -1)
	haveBiden = true;

      if(haveTrump && !haveBiden)
      {
	dataHadoop.set("0|0|1|1");
      }
      else if(!haveTrump && haveBiden)
      {
	dataHadoop.set("0|1|0|1");
      }
      else if(haveTrump && haveBiden)
      {
	dataHadoop.set("1|0|0|1");
      }
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

