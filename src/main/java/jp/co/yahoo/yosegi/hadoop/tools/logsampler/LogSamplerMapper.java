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

package jp.co.yahoo.yosegi.hadoop.tools.logsampler;

import jp.co.yahoo.yosegi.message.formatter.json.JacksonMessageWriter;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.reader.YosegiSchemaSpreadReader;
import jp.co.yahoo.yosegi.spread.Spread;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

public class LogSamplerMapper extends Mapper<NullWritable , Spread, LongWritable, Text> {

  private final JacksonMessageWriter writer = new JacksonMessageWriter();
  private final Random rnd = new Random();
  private long sampleCount = 0;
  private long spreads = 0; 

  @Override
  protected void setup( final  Context context ) throws IOException, InterruptedException {
    Configuration config = context.getConfiguration();
    sampleCount = config.getLong( "yosegi.tools.logsampler.samples" , 0 );
    spreads = config.getLong( "yosegi.tools.logsampler.spreads" , 0 );
    if ( spreads <= 0 ) {
      spreads = 1;
    }
  }

  @Override
  public void map(
      final NullWritable key ,
      final Spread value ,
      final Context context ) throws IOException, InterruptedException {
    YosegiSchemaSpreadReader reader = new YosegiSchemaSpreadReader( value );
    while ( reader.hasNext() ) {
      IParser parser = reader.next();
      long code = Math.abs( rnd.nextLong() ) % spreads;
      if ( sampleCount <= code ) {
        context.getCounter( "yosegi_sampler" , "skip" ).increment(1);
        continue;
      }
      Text result = new Text( writer.create( parser ) );
      context.getCounter( "yosegi_sampler" , "output" ).increment(1);
      context.write( new LongWritable( result.hashCode() ) , result );
    }
  }

}
