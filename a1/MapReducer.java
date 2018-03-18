import java.io.*;
import java.util.*;

import org.apache.hadoop.util.*;
import org.apache.log4j.Logger;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class MapReducer extends Configured implements Tool {

    public static class YTIDPair
            implements Writable, WritableComparable<YTIDPair> {
        private final Text yearTime = new Text();
        private final IntWritable id = new IntWritable();

        public YTIDPair() {

        }

        public YTIDPair(String yearTime, int id) {
            this.yearTime.set(yearTime);
            this.id.set(id);
        }
        
        @Override
        public void write(DataOutput out) throws IOException {
            yearTime.write(out);
            id.write(out);
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            yearTime.readFields(in);
            id.readFields(in);
        }

        @Override
        public int compareTo(YTIDPair pair) {
            if (yearTime.compareTo(pair.getYearTime()) == 0) {
                return id.compareTo(pair.id);
            }
            return yearTime.compareTo(pair.getYearTime());
        }

        public Text getYearTime() {
            return yearTime;
        }

        public IntWritable getId() {
            return id;
        }

    }

    public static class SalesMapper
            extends Mapper<LongWritable, Text, YTIDPair, Text> {
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String [] tokens = value.toString().trim().split(",");
            String yearTime = tokens[1] + "-" + tokens[2];
            int id = Integer.parseInt(tokens[0]);
            context.write(new YTIDPair(yearTime, id), new Text(tokens[2] + " " + tokens[0]));
        }
    }

    public static class SecondarySortSortingComparator
            extends WritableComparator {
        protected SecondarySortSortingComparator() {
            super(YTIDPair.class, true);
        }

        @Override
        public int compare(WritableComparable wc1, WritableComparable wc2) {
            YTIDPair pair = (YTIDPair) wc1;
            YTIDPair pair2 = (YTIDPair) wc2;
            return pair.compareTo(pair2);
        }
    }

    public static class SecondarySortPartitioner
            extends Partitioner<YTIDPair, IntWritable> {
        
        @Override
        public int getPartition(YTIDPair pair, IntWritable id, int numPartitions) {
            return Math.abs(pair.getYearTime().hashCode() % numPartitions);
        }
    }

    public static class SecondarySortGroupingComparator extends WritableComparator {
        public SecondarySortGroupingComparator() {
            super(YTIDPair.class, true);
        }

        @Override
        public int compare(WritableComparable wc1, WritableComparable wc2) {
            YTIDPair pair = (YTIDPair) wc1;
            YTIDPair pair2 = (YTIDPair) wc2;

            String year1 = pair.getYearTime().toString().split("-")[0];
            String year2 = pair2.getYearTime().toString().split("-")[0];

            return year1.compareTo(year2);
        }
    }

    public static class SalesReducer
            extends Reducer<YTIDPair, Text, Text, Text> {
        public void reduce(YTIDPair key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String year = key.getYearTime().toString().split("-")[0];
            
            String result = "";
            for (Text val : values) {
                String [] tokens = val.toString().split(" ");
                result += tokens[0] + " " + tokens[1] + ", ";
            }

            context.write(new Text(year), new Text(result));
        }
    }

    private static final Logger THE_LOGGER =
        Logger.getLogger(MapReducer.class);

    public int run(String [] args) throws Exception {
        Job job = Job.getInstance();
        job.setJarByClass(MapReducer.class);
        job.setJobName("MapReducer");
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(YTIDPair.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(SalesMapper.class);
        job.setPartitionerClass(SecondarySortPartitioner.class);
        job.setGroupingComparatorClass(SecondarySortGroupingComparator.class);
        job.setSortComparatorClass(SecondarySortSortingComparator.class);
        job.setReducerClass(SalesReducer.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        boolean status = job.waitForCompletion(true);
        THE_LOGGER.info("run(): status=" + status);
        return status ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: <input> <output>");
        }

        THE_LOGGER.info("inputDir = " + args[0]);
        THE_LOGGER.info("outputDir = " + args[1]);
        int returnStatus = ToolRunner.run(new MapReducer(), args);
        THE_LOGGER.info("returnStatus=" + returnStatus);
        System.exit(returnStatus);
    }
}
