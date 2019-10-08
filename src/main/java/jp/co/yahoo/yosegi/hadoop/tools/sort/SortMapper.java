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

import jp.co.yahoo.yosegi.message.formatter.json.JacksonMessageWriter;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.parser.IParser;
import jp.co.yahoo.yosegi.message.parser.json.JacksonMessageReader;
import jp.co.yahoo.yosegi.reader.YosegiSchemaSpreadReader;
import jp.co.yahoo.yosegi.spread.Spread;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Random;

public class SortMapper extends Mapper<NullWritable , Spread, Text, Text> {

  private final JacksonMessageWriter writer = new JacksonMessageWriter();
  private final JacksonMessageReader reader = new JacksonMessageReader();
  private final Random rnd = new Random();
  private IKeyParser keyParser;
  private IKeyParser tsParser;

  private static interface IKeyParser {
    String get( final IParser parser ) throws IOException;
  }

  private static class KeyParser implements IKeyParser {

    private final String nullValue;
    private final String[] keyArray;

    public KeyParser( final String[] keyArray , final String nullValue ) {
      this.keyArray = keyArray;
      this.nullValue = nullValue;
    }

    @Override
    public String get( final IParser parser ) throws IOException {
      IParser currentParser = parser;
      for ( int i = 0 ; i < keyArray.length - 1 ; i++ ) {
        currentParser = currentParser.getParser( keyArray[i] );
      }
      PrimitiveObject obj = currentParser.get( keyArray[keyArray.length - 1] );
      if ( obj == null ) {
        return nullValue;
      }
      return obj.getString();
    }

  }

  private static class EmptyKeyParser implements IKeyParser {
    @Override
    public String get( final IParser parser ) throws IOException {
      return new String();
    }
  }


  @Override
  protected void setup( final  Context context ) throws IOException, InterruptedException {
    Configuration config = context.getConfiguration();
    String sortKeyString = config.get( "yosegi.tools.sort.key" );
    IParser parseKeys = reader.create( sortKeyString );
    String[] keys = new String[parseKeys.size()];
    for ( int i = 0 ; i < parseKeys.size() ; i++ ) {
      keys[i] = parseKeys.get(i).getString();
    }
    keyParser = new KeyParser( keys , new String() );

    String tsKeyString = config.get( "yosegi.tools.sort.ts.key" , null );
    if ( tsKeyString == null || "".equals( tsKeyString ) ) {
      tsParser = new EmptyKeyParser();
    } else {
      IParser tsKeyParser = reader.create( tsKeyString );
      String[] tsKeys = new String[tsKeyParser.size()];
      for ( int i = 0 ; i < tsKeyParser.size() ; i++ ) {
        tsKeys[i] = tsKeyParser.get(i).getString();
      }
      tsParser = new KeyParser( tsKeys , new String( "0" ) );
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
      Text outKey = new Text(
          String.format( "%s%s" , keyParser.get( parser ) , tsParser.get( parser ) ) );
      context.write( outKey , new Text( writer.create( parser ) ) );
    }
  }

}
