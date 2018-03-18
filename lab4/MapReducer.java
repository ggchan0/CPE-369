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

    public static class Product implements Comparable<Product> {
        private final int id;        
        private final String info;
        private final double price;
        

        public Product(int id, String info, double price) {
            this.id = id;
            this.info = info;
            this.price = price;
        }

        @Override
        public int compareTo(Product p) {
            if (this.price > p.price) {
                return -1;
            } else if (this.price < p.price) {
                return 1;
            } else {
                return 0;
            }
        }

        public int getId() {
            return id;
        }

        public String getInfo() {
            return info;
        }

        public double getPrice() {
            return price;
        }

        public String toString() {
            return this.id + "," + this.info + "," + this.price;
        }

    }

    public static class ProductMapper
            extends Mapper<LongWritable, Text, NullWritable, Text> {

        public static final int DEFAULT_N = 10;
        private int n = DEFAULT_N;
        private TreeSet<Product> top = new TreeSet<Product>();
        
        @Override
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String [] tokens = value.toString().trim().split(",");
            int id = Integer.parseInt(tokens[0]);
            double price = Double.parseDouble(tokens[2]);
            top.add(new Product(id, tokens[1], price));
            if (top.size() > n) {
                top.remove(top.last());
            }
        }

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            this.n = context.getConfiguration().getInt("N", DEFAULT_N);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            for (Product p: top) {
                context.write(NullWritable.get(), new Text(p.toString()));
            }
        }
    }

    public static class ProductReducer
            extends Reducer<NullWritable, Text, NullWritable, Text> {

        private int n = ProductMapper.DEFAULT_N;
        private SortedSet<Product> top = new TreeSet<>();

        @Override
        public void reduce(NullWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            for (Text val : values) {
                String [] tokens = val.toString().trim().split(",");
                int id = Integer.parseInt(tokens[0]);
                double price = Double.parseDouble(tokens[2]);
                top.add(new Product(id, tokens[1], price));
                if (top.size() > n) {
                    top.remove(top.last());
                }
            }

            for (Product val : top) {
                context.write(NullWritable.get(), new Text(val.toString()));
            }
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            this.n = context.getConfiguration().getInt("N", ProductMapper.DEFAULT_N);
        }
    }

    private static final Logger THE_LOGGER =
        Logger.getLogger(MapReducer.class);

    public int run(String [] args) throws Exception {
        Job job = Job.getInstance();
        job.getConfiguration().setInt("N", 10);
        job.setJarByClass(MapReducer.class);
        job.setJobName("MapReducer");
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setMapperClass(ProductMapper.class);
        job.setReducerClass(ProductReducer.class);
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
