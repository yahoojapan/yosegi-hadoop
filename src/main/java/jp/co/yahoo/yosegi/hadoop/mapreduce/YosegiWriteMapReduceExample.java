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
