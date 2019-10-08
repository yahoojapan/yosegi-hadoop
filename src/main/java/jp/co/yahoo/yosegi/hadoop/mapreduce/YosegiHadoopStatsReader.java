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

import jp.co.yahoo.yosegi.reader.YosegiStatsReader;
import jp.co.yahoo.yosegi.stats.SpreadSummaryStats;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public class YosegiHadoopStatsReader extends RecordReader<NullWritable, List<SpreadSummaryStats>> {

  private final YosegiStatsReader currentReader = new YosegiStatsReader();
  private boolean hasNext = true;

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public List<SpreadSummaryStats> getCurrentValue() throws IOException, InterruptedException {
    return currentReader.getSpreadSummaryStatsList();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if ( hasNext ) {
      hasNext = false;
      return true;
    }
    return false;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return 0;
  }

  @Override
  public void initialize(
      final InputSplit inputSplit ,
      final TaskAttemptContext context ) throws IOException, InterruptedException {
    FileSplit fileSplit = (FileSplit)inputSplit;
    Configuration config = context.getConfiguration();
    Path path = fileSplit.getPath();
    FileSystem fs = path.getFileSystem( config );
    long fileLength = fs.getLength( path );
    long start = fileSplit.getStart();
    long length = fileSplit.getLength();
    InputStream in = fs.open( path );
    setStream( in , fileLength , start , length );
  }

  public void setStream(
      final InputStream in ,
      final long fileLength ,
      final long start ,
      final long length ) throws IOException {
    currentReader.readStream(
        in , fileLength , new jp.co.yahoo.yosegi.config.Configuration() , start , length );
  }

  @Override
  public void close() throws IOException {}

}
