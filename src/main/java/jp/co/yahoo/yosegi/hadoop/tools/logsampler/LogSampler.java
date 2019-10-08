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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

public class LogSampler extends Configured implements Tool {

  // from Hadoop's block size
  public static final String DEFAULT_SPREAD_COUNT = "5";

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
        .create( 'o' );

    Option spread = OptionBuilder
        .withLongOpt("spread")
        .withDescription("Number of Spread.")
        .withArgName("spread")
        .create( 's' );

    Option help = OptionBuilder
        .withLongOpt("help")
        .withDescription("help")
        .withArgName("help")
        .create( 'h' );

    Options  options = new Options();

    return options
        .addOption( input )
        .addOption( output )
        .addOption( spread )
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
      throw new IOException( "output path is not exists. " + output );
    }
    return outputPath;
  }

  /**
   * Get spread count.
   */
  public long getSpreadCount( final Path inputPath )
      throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = getConf();
    Job job = Job.getInstance( conf, "Yosegi get spread count." );
    job.setMapperClass( GetStatsMapper.class );
    job.setNumReduceTasks(0);
    job.setOutputKeyClass( NullWritable.class );
    job.setOutputValueClass( NullWritable.class );
    job.setInputFormatClass( YosegiStatsInputFormat.class );
    YosegiStatsInputFormat.addInputPath( job , inputPath );
    job.setOutputFormatClass( NullOutputFormat.class );
    if ( ! job.waitForCompletion(true) ) {
      throw new IOException( "Failed get spread count." );
    }
    return job.getCounters().findCounter( "yosegi_stats" , "spreads" ).getValue();
  }

  /**
   * Create sample file.
   */
  public void createSampleFile(
      final Path inputPath ,
      final Path outputPath ,
      final long sampleCount ,
      final long spreadCount ) throws IOException, InterruptedException, ClassNotFoundException {
    Configuration conf = getConf();
    conf.setLong( "yosegi.tools.logsampler.samples" , sampleCount );
    conf.setLong( "yosegi.tools.logsampler.spreads" , spreadCount );
    Job job = Job.getInstance( conf, "Yosegi create sample file." );
    job.setMapperClass( LogSamplerMapper.class );
    job.setReducerClass( LogSamplerReducer.class );
    job.setNumReduceTasks(1);
    job.setInputFormatClass( YosegiSpreadInputFormat.class );
    YosegiSpreadInputFormat.addInputPath( job , inputPath );
    job.setOutputFormatClass( YosegiParserOutputFormat.class );
    YosegiParserOutputFormat.setOutputPath( job, outputPath );

    if ( ! job.waitForCompletion(true) ) {
      throw new IOException( "Failed create sample file." );
    }
  }

  /**
   * Output usage.
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

    Path inputPath = createInputDirectoryPath( cl.getOptionValue( "input" , null ) );

    String output = cl.getOptionValue( "output" , null );
    Path outputPath = createOutputDirectoryPath( output );
    long sampleCount =  Long.parseLong( cl.getOptionValue( "spread" , DEFAULT_SPREAD_COUNT ) );

    long spreadCount = getSpreadCount( inputPath );

    createSampleFile( inputPath , outputPath , sampleCount , spreadCount );

    return 0;
  }

  /**
   * Run MapReduce.
   */
  public static void main( final String[] args ) throws Exception {
    System.exit( ToolRunner.run( new LogSampler() ,  args ) );
  }

}
