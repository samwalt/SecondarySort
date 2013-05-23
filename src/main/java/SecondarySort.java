import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.lib.InputSampler;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SecondarySort extends Configured implements Tool {

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SecondarySort(), args);
		System.out.println("退出代码" + exitCode);
	}

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.println("需要两个参数：输入目录名和输出目录名");
			System.err.println("实际参数数量：" + args.length);
			for (String arg : args) {
				System.err.print(arg + " ");
			}
			System.err.println();
			return -1;
		}

		JobConf conf = new JobConf(getConf(), getClass());
		conf.setJobName("全排序 演示");

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.setMapperClass(OrderMapper.class);
		conf.setReducerClass(OrderReducer.class);

		conf.setPartitionerClass(FirstPartitioner.class);
		conf.setOutputKeyComparatorClass(KeyComparator.class);
		conf.setOutputValueGroupingComparator(GroupComparator.class);

		/** mapper输出的key类型和mapper输入的key类型不一样，需要指定输出的key的java class */
		conf.setMapOutputKeyClass(Pair.class);
		/** 取样器 */
		InputSampler.Sampler<Pair, NullWritable> sampler = new InputSampler.RandomSampler<Pair, NullWritable>(
				0.1, 200, 2);

		// dfs上的输入路径
		Path inputPath = FileInputFormat.getInputPaths(conf)[0];
		inputPath = inputPath.makeQualified(inputPath.getFileSystem(conf));

		// 取样文件
		Path samplerFile = new Path(inputPath, "_sampler");
		TotalOrderPartitioner.setPartitionFile(conf, samplerFile);
		InputSampler.writePartitionFile(conf, sampler);

		/** 取样文件放到分布式缓存，给各节点共享 */
		/*
		 * URI samplerUri = new URI(samplerFile.toString()+"#_sampler");
		 * DistributedCache.addCacheFile(samplerUri, conf);
		 * DistributedCache.createSymlink(conf);
		 */

		JobClient.runJob(conf);
		return 0;
	}

	static class OrderMapper extends MapReduceBase implements
			Mapper<LongWritable, Text, Pair, NullWritable> {

		public void map(LongWritable key, Text value,
				OutputCollector<Pair, NullWritable> output, Reporter reporter)
				throws IOException {
			String line = value.toString().trim();
			String[] items = line.split(" ");
			output.collect(new Pair(Integer.parseInt(items[1]), items[0]),
					NullWritable.get());
		}

	}

	static class OrderReducer extends MapReduceBase implements
			Reducer<Pair, NullWritable, IntWritable, Text> {

		public void reduce(Pair key, Iterator<NullWritable> values,
				OutputCollector<IntWritable, Text> output, Reporter reporter)
				throws IOException {
			while (values.hasNext()) {
				output.collect(new IntWritable(key.getFirst()), new Text(key.getSecond()));
			}
		}
	}

	static class Pair implements WritableComparable<Pair> {

		int first;
		String second;

		public Pair() {
		}

		public Pair(int first, String second) {
			this.first = first;
			this.second = second;
		}

		public int getFirst() {
			return first;
		}

		public String getSecond() {
			return second;
		}

		public void write(DataOutput out) throws IOException {
			out.writeInt(first);
			out.writeUTF(second);
		}

		public void readFields(DataInput in) throws IOException {
			first = in.readInt();
			second = in.readUTF();
		}

		public int compareTo(Pair o) {
			if (first != o.getFirst()) {
				return first < o.getFirst() ? -1 : 1;
			}
			if (!second.equals(o.getSecond())) {
				return second.compareTo(o.getSecond()) < 0 ? -1 : 1;
			}
			return 0;
		}

		public int hashCode() {
			return first * 157;
		}

		public boolean equals(Object o) {
			if (o == null)
				return false;
			if (this == o)
				return true;
			if (o instanceof Pair) {
				Pair r = (Pair) o;
				return r.first == first && r.second.equals(second);
			} else {
				return false;
			}
		}

	}

	static class KeyComparator extends WritableComparator {

		protected KeyComparator() {
			super(Pair.class, true);
		}

		public int compare(WritableComparable w1, WritableComparable w2) {
			Pair p1 = (Pair) w1;
			Pair p2 = (Pair) w2;
			int cmp = p1.getFirst() - p2.getFirst();
			if (cmp != 0) {
				return cmp;
			}
			return -1 * (p1.getSecond().compareTo(p2.getSecond()));
		}
	}

	static class GroupComparator extends WritableComparator {
		protected GroupComparator() {
			super(Pair.class, true);
		}

		public int compare(WritableComparable w1, WritableComparable w2) {
			Pair p1 = (Pair) w1;
			Pair p2 = (Pair) w2;

			return p1.getFirst() - p2.getFirst();
		}
	}

	static class FirstPartitioner implements
			Partitioner<SecondarySort.Pair, NullWritable> {
		public int getPartition(SecondarySort.Pair key, NullWritable value,
				int numPartitions) {
			return Math.abs(key.getFirst() * 127) % numPartitions;
		}

		public void configure(JobConf job) {
		}
	}
}
