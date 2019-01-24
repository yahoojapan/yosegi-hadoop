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

import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.writer.YosegiRecordWriter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.io.OutputStream;

public class YosegiParserRecordWriter extends RecordWriter<NullWritable, IParser> {

  private final YosegiRecordWriter writer;

  public YosegiParserRecordWriter(
      final OutputStream out , final Configuration config ) throws IOException {
    writer = new YosegiRecordWriter( out , config );
  }

  @Override
  public void close( final TaskAttemptContext context ) throws IOException, InterruptedException {
    writer.close();
  }

  @Override
  public void write(
      final NullWritable key , final IParser value ) throws IOException, InterruptedException {
    writer.addParserRow( value );
  }

}
