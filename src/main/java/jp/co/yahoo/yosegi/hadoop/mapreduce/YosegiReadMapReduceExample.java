/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
