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

import jp.co.yahoo.yosegi.hadoop.mapreduce.YosegiParserOutputFormat;
import jp.co.yahoo.yosegi.hadoop.mapreduce.YosegiSpreadInputFormat;
import jp.co.yahoo.yosegi.hadoop.mapreduce.YosegiStatsInputFormat;
import jp.co.yahoo.yosegi.message.parser.IParser;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class Sort extends Configured implements Tool {

  private static final long FILE_SIZE = 1024 * 1024 * 128;

  /**
   * Create options.
   */
  public static Options createOptions( final String[] args ) {
    Option input = OptionBuilder
        .withLongOpt("input")
        .withDescription("Input hdfs directory path.")
        .hasArg()
        .isRequired()
        .withArgName("input")
        .create( 'i' );

    Option output = OptionBuilder
        .withLongOpt("output")
        .withDescription("Output hdfs directory path.")
        .hasArg()
        .withArgName("output")
        .isRequired()
        .create( 'o' );

    Option key = OptionBuilder
        .withLongOpt("key")
        .withDescription("Target sort key.")
        .hasArg()
        .withArgName("key")
        .isRequired()
        .create( 'k' );

    Option field = OptionBuilder
        .withLongOpt("ts_key")
        .withDescription("Timestamp filed path.")
        .withArgName("ts_key")
        .hasArg()
        .create( 't' );

    Option window = OptionBuilder
        .withLongOpt("window")
        .withDescription("Window size")
        .withArgName("window")
        .hasArg()
        .create( 'w' );

    Option ts = OptionBuilder
        .withLongOpt("timestamp_type")
        .withDescription("sec or msec")
        .withArgName("timestamp_type")
        .hasArg()
        .create( 'f' );

    Option help = OptionBuilder
        .withLongOpt("help")
        .withDescription("help")
        .withArgName("help")
        .create( 'h' );

    Options  options = new Options();

    return options
        .addOption( input )
        .addOption( output )
        .addOption( key )
        .addOption( ts )
        .addOption( window )
        .addOption( field )
        .addOption( help );

  }

  /**
   * Create input Path object.
   */
  public Path createInputDirectoryPath( final String input ) throws IOException {
    if ( input == null || input.isEmpty() ) {
      throw new IOException( "input is null or empty." );
    }
    FileSystem fs = FileSystem.get( getConf() );
    Path inputPath = new Path( input );

    if ( ! fs.exists( inputPath ) ) {
      throw new IOException( "input path is not exists. " + input );
    }

    if ( ! fs.isDirectory( inputPath ) ) {
      throw new IOException( "input path is not directory. " + input );
    }
    return inputPath;
  }

  /**
   * Create output Path object.
   */
  public Path createOutputDirectoryPath( final String output ) throws IOException {
    if ( output == null || output.isEmpty() ) {
      throw new IOException( "output is null or empty." );
    }
    FileSystem fs = FileSystem.get( getConf() );
    Path outputPath = new Path( output );

    if ( fs.exists( outputPath ) ) {
      throw new IOException( "output path is exists. " + output );
    }
    return outputPath;
  }

  /**
   * Get spread count.
   */
  public long getRealDataSize( final Path inputPath )
      throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = getConf();
    Job job = Job.getInstance( conf, "Yosegi get real data size." );
    job.setMapperClass( GetStatsMapper.class );
    job.setNumReduceTasks(0);
    job.setOutputKeyClass( NullWritable.class );
    job.setOutputValueClass( NullWritable.class );
    job.setInputFormatClass( YosegiStatsInputFormat.class );
    YosegiStatsInputFormat.addInputPath( job , inputPath );
    job.setOutputFormatClass( NullOutputFormat.class );
    if ( ! job.waitForCompletion(true) ) {
      throw new IOException( "Failed get real data size." );
    }
    return job.getCounters().findCounter( "yosegi_stats" , "real_data_size" ).getValue();
  }

  /**
   * Create sample file.
   */
  public void createFile(
      final Path inputPath ,
      final Path outputPath ,
      final long realDataSize ,
      final String sortKey,
      final String tsKey ,
      final long windowSize ) throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = getConf();
    conf.set( "yosegi.tools.sort.key" , sortKey );
    conf.set( "yosegi.tools.sort.ts.key" , tsKey );
    conf.setLong( "yosegi.tools.sort.ts.window" , windowSize );
    int reducer = (int)( realDataSize / FILE_SIZE );
    Job job = Job.getInstance( conf, "Yosegi create sort file." );
    job.setMapperClass( SortMapper.class );
    job.setMapOutputKeyClass( Text.class );
    job.setMapOutputValueClass( Text.class );
    job.setReducerClass( SortReducer.class );
    job.setNumReduceTasks( reducer );
    job.setInputFormatClass( YosegiSpreadInputFormat.class );
    YosegiSpreadInputFormat.addInputPath( job , inputPath );
    job.setOutputFormatClass( YosegiParserOutputFormat.class );
    YosegiParserOutputFormat.setOutputPath( job, outputPath );
    job.setPartitionerClass( SortPartitioner.class );

    if ( ! job.waitForCompletion(true) ) {
      throw new IOException( "Failed create sample file." );
    }
  }

  /**
   * Create input file list.
   */
  public static void printHelp( final String[] args ) {
    HelpFormatter hf = new HelpFormatter();
    hf.printHelp( "[options]" , createOptions( args ) );
  }

  @Override
  public int run( final String[] args )
      throws IOException, InterruptedException, ClassNotFoundException {
    CommandLine cl;
    try {
      CommandLineParser clParser = new GnuParser();
      cl = clParser.parse( createOptions( args ) , args );
    } catch ( ParseException ex ) {
      printHelp( args );
      throw new IOException( ex );
    }

    if ( cl.hasOption( "help" ) ) {
      printHelp( args );
      return 0;
    }

    String timestampType = cl.getOptionValue( "timestamp_type" , "sec" );
    if ( timestampType == null || timestampType.isEmpty() ) {
      throw new IOException( "Invalid timestamp type." );
    }
    long windowSize;
    if ( "msec".equals( timestampType ) ) {
      windowSize = Long.parseLong( cl.getOptionValue( "window" , "600000" ) );
    } else if ( "sec".equals( timestampType ) ) {
      windowSize = Long.parseLong( cl.getOptionValue( "window" , "600" ) );
    } else {
      throw new IOException( "Invalid timestamp type. " + timestampType );
    }
    String sortKey = cl.getOptionValue( "key" , "" );
    if ( sortKey.isEmpty() ) {
      throw new IOException( "sort key is null or empty." );
    }
    String tsKey = cl.getOptionValue( "ts_key" , "" );

    Path inputPath = createInputDirectoryPath( cl.getOptionValue( "input" , null ) );
    Path outputPath = createOutputDirectoryPath( cl.getOptionValue( "output" , null ) );

    long realDataSize = getRealDataSize( inputPath );

    createFile( inputPath , outputPath , realDataSize , sortKey , tsKey , windowSize );

    return 0;
  }

  /**
   * Run MapReduce.
   */
  public static void main( final String[] args ) throws Exception {
    System.exit( ToolRunner.run( new Sort() ,  args ) );
  }

}
