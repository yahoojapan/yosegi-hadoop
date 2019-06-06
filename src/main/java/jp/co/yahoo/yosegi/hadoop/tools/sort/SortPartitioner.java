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

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.Random;

public class SortPartitioner extends Partitioner<Text, Text> implements Configurable {

  private final Random rnd = new Random();
  private Configuration config;
  private long windowSize;

  @Override
  public Configuration getConf() {
    return config;
  }

  @Override
  public void setConf( final Configuration config ) {
    windowSize = config.getLong( "yosegi.tools.sort.ts.window" , 60000 );
    this.config = config;
  }

  @Override
  public int getPartition( final Text key, final Text value, final int numPartitions ) {
    String[] data = key.toString().split( "" );
    
    if ( data.length == 0 || data[0].length() == 0 ) {
      return rnd.nextInt( numPartitions );
    }

    if ( data.length != 2 || data[1].length() == 0 ) {
      return Math.abs( data[0].hashCode() % numPartitions );
    }

    try {
      long bucket = Long.parseLong( data[1] ) / windowSize;
      return Math.abs( String.format( "%s%d" , data[0] , bucket ).hashCode() % numPartitions );
    } catch ( NumberFormatException ex ) {
      return Math.abs( key.hashCode() % numPartitions );
    }

  }
}
