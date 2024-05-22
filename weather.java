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
        conf.setOutputValueClass(DoubleWritable.class);
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
public class mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,DoubleWritable>
{
    public void map(LongWritable key,Text value,OutputCollector<Text,DoubleWritable> output,Reporter r) throws IOException
    {
        String line=value.toString();
        String year=line.substring(15,19);
        Double temp=Double.parseDouble(line.substring(87,92));
        output.collect(new Text(year),new DoubleWritable(temp));
    }
}

//reducer

package weather;
import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
public class reducer extends MapReduceBase implements Reducer<Text,DoubleWritable,Text,DoubleWritable>
{
    public void reduce(Text key,Iterator<DoubleWritable> value,OutputCollector<Text,DoubleWritable> output,Reporter r) throws IOException
    {
          Double max=-9999.0,min=9999.0;
          while(value.hasNext())
          {
            Double temp=value.next().get();
            max=Math.max(temp,max);
            min=Math.min(temp,min);
          }
          output.collect(key,new DoubleWritable(max));
          output.collect(key,new DoubleWritable(min));
    }
}
