<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->
# Preparation
Please create Hadoop installation and execution environment of MapReduce.

[Apache Hadoop](https://hadoop.apache.org/)


## Preparation of input data.
In this example, JSON data is assumed to be input data.
With this data as input, we create a table in Yosegi format and read data from the table.

Create the following csv file.In this example, to illustrate writing to and reading from the table, the data is a simple example.
If you already have a table, you still have the input.

```
hadoop dfs -put - /tmp/example.json <<__JSON__
{"id":"X_0001","name":"AAA","age":20}
{"id":"X_0002","name":"BBB","age":30}
{"id":"X_0003","name":"CCC","age":32}
{"id":"X_0004","name":"DDD","age":21}
{"id":"X_0005","name":"EEE","age":28}
{"id":"X_0006","name":"FFF","age":21}
__JSON__
```

## Create yosegi file
Load json file and create yosegi file.

The sample code for MapReduce is this.
This class is included in the jar.

```
package jp.co.yahoo.yosegi.hadoop.mapreduce;

import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;
import jp.co.yahoo.yosegi.reader.YosegiSchemaSpreadReader;
import jp.co.yahoo.yosegi.spread.Spread;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class YosegiWriteMapReduceExample extends Configured implements Tool {

  public static class YosegiInputMapper extends Mapper<LongWritable, Text, NullWritable, IParser> {

    private final JacksonMessageReader reader = new JacksonMessageReader();

    @Override
    public void map(
        final LongWritable key ,
        final Text value ,
        final Context context ) throws IOException, InterruptedException {
      context.write(
          NullWritable.get() , reader.create( value.getBytes() , 0 , value.getLength() ) );
    }

  }

  @Override
  public int run( final String[] args )
      throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = getConf();
    Job job = Job.getInstance( conf, "Yosegi write example." );
    job.setJarByClass( YosegiWriteMapReduceExample.class );
    job.setMapperClass( YosegiInputMapper.class );
    job.setOutputKeyClass( NullWritable.class );
    job.setOutputValueClass( IParser.class );
    job.setInputFormatClass( TextInputFormat.class );
    job.setOutputFormatClass( YosegiParserOutputFormat.class );
    job.setNumReduceTasks(0);
    TextInputFormat.addInputPath(job, new Path(args[0]));
    YosegiParserOutputFormat.setOutputPath(job, new Path(args[1]));
    return job.waitForCompletion(true) ? 0 : 1;
  }

  /**
   * Run MapReduce.
   */
  public static void main( final String[] args ) throws Exception {
    System.exit( ToolRunner.run( new YosegiWriteMapReduceExample() ,  args ) );
  }

}
```

Run MapReduce.

```
CLASSPATH=./yosegi.jar yarn jar yosegi-hadoop-0.9.0.jar jp.co.yahoo.yosegi.hadoop.mapreduce.YosegiWriteMapReduceExample -libjars yosegi.jar /tmp/example.json  /tmp/example_yosegi
```

## Read yosegi file
Read as a data frame from yosegi file.

The sample code for MapReduce is this.
This class is included in the jar.

```
package jp.co.yahoo.yosegi.hadoop.mapreduce;

import jp.co.yahoo.yosegi.message.formatter.json.JacksonMessageWriter;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.reader.YosegiSchemaSpreadReader;
import jp.co.yahoo.yosegi.spread.Spread;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class YosegiReadMapReduceExample extends Configured implements Tool {

  public static class YosegiInputMapper extends Mapper<NullWritable , Spread, NullWritable, Text> {

    private final JacksonMessageWriter writer = new JacksonMessageWriter();

    @Override
    public void map(
        final NullWritable key ,
        final Spread value ,
        final Context context ) throws IOException, InterruptedException {
      YosegiSchemaSpreadReader reader = new YosegiSchemaSpreadReader( value );
      while ( reader.hasNext() ) {
        IParser parser = reader.next();
        Text result = new Text( writer.create( parser ) );
        context.write( NullWritable.get() , result );
      }
    }

  }

  public static class YosegiOutputReducer
      extends Reducer<NullWritable , Text, NullWritable , Text> {

    @Override
    public void reduce(
        final NullWritable key ,
        final Iterable<Text> values ,
        final Context context ) throws IOException, InterruptedException {
      for ( Text val : values ) {
        context.write( key , val );
      }
    }

  }

  @Override
  public int run( final String[] args )
      throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = getConf();
    Job job = Job.getInstance( conf, "Yosegi read example." );
    job.setJarByClass( YosegiReadMapReduceExample.class );
    job.setMapperClass( YosegiInputMapper.class );
    job.setReducerClass( YosegiOutputReducer.class );
    job.setOutputKeyClass( NullWritable.class );
    job.setOutputValueClass(Text.class);
    job.setInputFormatClass( YosegiCombineSpreadInputFormat.class );
    YosegiSpreadInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    return job.waitForCompletion(true) ? 0 : 1;
  }

  /**
   * Run MapReduce.
   */
  public static void main( final String[] args ) throws Exception {
    System.exit( ToolRunner.run( new YosegiReadMapReduceExample() ,  args ) );
  }

}
```

Run MapReduce.

```
yarn jar yosegi-hadoop-0.9.0.jar jp.co.yahoo.yosegi.hadoop.mapreduce.YosegiReadMapReduceExample -libjars
yosegi.jar /tmp/example_yosegi /tmp/example_to_json
```

It is output in JSON.
```
-bash-4.2$ hdfs dfs -text /tmp/example_to_json/part-r-00000*
{"name":"FFF","id":"X_0006","age":21}
{"name":"EEE","id":"X_0005","age":28}
{"name":"DDD","id":"X_0004","age":21}
{"name":"CCC","id":"X_0003","age":32}
{"name":"BBB","id":"X_0002","age":30}
{"name":"AAA","id":"X_0001","age":20}
```

# What if I want to know more?
