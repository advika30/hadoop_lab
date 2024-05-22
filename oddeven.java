//driver
package oddeven;
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
package oddeven;
import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
public class mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>
{
    public void map(LongWritable key,Text value,OutputCollector<Text,IntWritable> output,Reporter r) throws IOException
    {
        String[] line=value.toString.split(" ");
        for(String num:line)
        {
            int n=Integer.parseInt(num);
            if(n%2==0)
            {
                output.collect(new Text("even"),new IntWritable(n));
            }
            else
            {
                output.collect(new Text("odd"),new IntWritable(n));
            }
        }
    }
}

//reducer

package oddeven;
import java.io.*;
import java.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.io.*;
public class reducer extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>
{
    public void reduce(Text key,Iterator<IntWritable> value,OutputCollector<Text,IntWritable> output,Reporter r) throws IOException
    {
          int s=0,c=0;
          while(value.hasNext())
          {
            s+=value.next().get();
            c++;
          }
          output.collect(new Text("sum is"),new IntWritable(s));
          output.collect(new Text("count is"),new IntWritable(c));
    }
}
