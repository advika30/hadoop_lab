//driver
package weather;
import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.Path;
public class driver
{
     public static void main(String args[]) throws IOException
     {
        JobConf conf=new JobConf(driver.class);
        conf.setMapperClass(mapper.class);
        conf.setReducerClass(reducer.class);
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(conf,new Path(args[0]));
        FileOutputFormat.setOutputPath(conf,new Path(args[1]));
        JobClient.runJob(conf);
     }
}


//mapper
package weather;
import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
public class mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>
{
    public void map(LongWritable key,Text value,OutputCollector<Text,IntWritable> output,Reporter r) throws IOException
    {
        String[] line=value.toString().split(",");
        output.collect(new Text(line[2]),new IntWritable(1));
    }
}

//reducer

package weather;
import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
public class reducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>
{
    public void reduce(Text key,Iterator<IntWritable> value,OutputCollector<Text,IntWritable> output,Reporter r) throws IOException
    {
        int s=0;
          while(value.hasNext())
          {
            s+=value.next().get();
          }
          output.collect(key,new IntWritable(s));
    }
}
