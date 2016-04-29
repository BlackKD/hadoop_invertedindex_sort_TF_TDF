package com.hadoop.InvertIndex;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;meimport org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * Created by jiangchenzhou on 16/4/26.
 */
public class TFIDF {

    public static double FileNum = 219;

    public static class TFIDFMapper extends Mapper<Object,Text,Text,Text> {
        private FileSplit split;
        public void map(Object key, Text value,Context context)
                throws IOException, InterruptedException {
s            split = (FileSplit) context.getInputSplit();
            StringTokenizer stk = new StringTokenizer(value.toString());
            stk.nextToken();
            String word = stk.nextToken();
            ArrayList<String> wordInfo = new ArrayList<String>();
            while(stk.hasMoreTokens()) {
                String temp = stk.nextToken();
                wordInfo.add(temp);
            }

            System.out.println(FileNum + " " + wordInfo.size());
            System.out.println(wordInfo);
            double IDF = Math.log(FileNum/(double)(wordInfo.size()+1));
            String IDFString = Double.toString(IDF);
            for (String info: wordInfo) {
                String [] temp = info.split(";");
                String [] temp1 = temp[0].split(":");
                String times = temp1[1];
                String [] temp2 = temp1[0].split("[0-9]");
                String author = temp2[0];
                String bookName = temp2[2];
                Text keyInfo = new Text(author + " " + word + " " + bookName + " ");
                Text valueInfo = new Text(times + " " + IDFString);
                context.write(keyInfo, valueInfo);
            }
        }
    }

    public static class TFIDFCombiner extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            StringTokenizer keyStk = new StringTokenizer(key.toString());
            Text keyInfo = new Text(keyStk.nextToken()+" " + keyStk.nextToken());
            for (Text value: values) {
                context.write(keyInfo, value);
            }
        }
    }

    public static class TFIDFReduce extends Reducer<Text,Text,Text,DoubleWritable> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            int TF = 0;
            double IDF = 0;
            for (Text value: values) {
                StringTokenizer stk = new StringTokenizer(value.toString());
                TF += Integer.parseInt(stk.nextToken());
                IDF = Double.parseDouble(stk.nextToken());
            }
            StringTokenizer stk = new StringTokenizer(key.toString());
            context.write(new Text(stk.nextToken()+" " + stk.nextToken() + " "), new DoubleWritable(TF * IDF));
        }
    }


    public static void main(String[] args)  throws IOException, InterruptedException, ClassNotFoundException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] inputFiles = fs.listStatus(new Path(args[0]));
        FileNum = inputFiles.length;
        Job TFIDFJob = new Job(conf, "TFIDF");
        TFIDFJob.setJarByClass(TFIDF.class);
        FileInputFormat.addInputPath(TFIDFJob, new Path(args[1]));
        FileOutputFormat.setOutputPath(TFIDFJob, new Path("TF-IDF"));
        TFIDFJob.setMapperClass(TFIDFMapper.class);
        TFIDFJob.setMapOutputKeyClass(Text.class);
        TFIDFJob.setMapOutputValueClass(Text.class);
        TFIDFJob.setCombinerClass(TFIDFCombiner.class);
        TFIDFJob.setReducerClass(TFIDFReduce.class);
        TFIDFJob.setOutputKeyClass(Text.class);
        TFIDFJob.setOutputKeyClass(DoubleWritable.class);

        System.exit(TFIDFJob.waitForCompletion(true) ? 0 : 1);

    }
}
