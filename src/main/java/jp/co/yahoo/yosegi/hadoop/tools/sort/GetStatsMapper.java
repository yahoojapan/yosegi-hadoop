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

package jp.co.yahoo.yosegi.hadoop.tools.sort;

import jp.co.yahoo.yosegi.stats.SpreadSummaryStats;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.List;

public class GetStatsMapper
    extends Mapper<NullWritable , List<SpreadSummaryStats>, NullWritable, NullWritable> {

  @Override
  public void map(
      final NullWritable key ,
      final List<SpreadSummaryStats> value ,
      final Context context ) throws IOException, InterruptedException {
    Counter counter = context.getCounter( "yosegi_stats" , "real_data_size" );
    for ( SpreadSummaryStats stats : value ) {
      counter.increment( stats.getSummaryStats().getRealDataSize() );
    }
  }

}
