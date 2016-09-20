import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Scanner;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class anagram {

	public static String getHashStringValue(String a) {
		char[] list = a.toCharArray();
		Arrays.sort(list);
		int num = 0;
		StringBuilder sbd = new StringBuilder();
		for (int i = 0; i < list.length; i++) {
			if (i == list.length - 1) {
				num++;
				sbd.append(list[i]);
				sbd.append(num);
			} else {
				if (list[i] < list[i + 1]) {
					num++;
					sbd.append(list[i]);
					sbd.append(num);
					num = 0;
				} else {
					num++;
				}
			}
		}
		return sbd.toString();
	}

	public static class FirstMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				String str = itr.nextToken();
				Text word = new Text(str);
				Text text = new Text(getHashStringValue(str));
				context.write(text, word);
			}
		}
	}

	public static class FirstReducer extends Reducer<Text, Text, NullWritable, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String list = "";
			// System.out.println("first reducer: " + key + " " +
			// values.toString());
			for (Text val : values) {
				list += val.toString() + " ";
				// System.out.println(key + " " + val.toString());
			}
			Text result = new Text(list);
			context.write(NullWritable.get(), result);
		}
	}

	public static class SecondMapper extends Mapper<Object, Text, IntWritable, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// System.out.println("second mapper");
			String line;
			Scanner scan = new Scanner(value.toString());
			while (scan.hasNext()) {
				line = scan.nextLine();
				Text word = new Text(line);
				IntWritable len = new IntWritable(-line.split(" ").length);
				// System.out.println("second mapper " + len + " " + line);
				context.write(len, word);
			}
		}
	}

	public static class SecondReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			// System.out.println("second reducer " + key + " " +
			// values.toString());
			for (Text value : values) {
				// System.out.println("second reducer " + key + " " + value);
				context.write(NullWritable.get(), value);
			}
		}

	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		if (otherArgs.length != 3) {
			System.err.println("Usage: wordcount <in> <inter> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "separate anagram");

		job.setJarByClass(anagram.class);
		job.setMapperClass(FirstMapper.class);
		job.setReducerClass(FirstReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		Path intermedia_path = new Path(otherArgs[1]);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, intermedia_path);

		System.out.println("pending job1");
		job.waitForCompletion(true);
		System.out.println("finish job1");

		Job job2 = new Job(conf, "sort anagram");
		job2.setJarByClass(anagram.class);
		job2.setMapperClass(SecondMapper.class);
		job2.setReducerClass(SecondReducer.class);

		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Text.class);
		job2.setOutputKeyClass(NullWritable.class);
		job2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job2, intermedia_path);
		FileOutputFormat.setOutputPath(job2, new Path(otherArgs[2]));

		System.out.println("pending job2");
		job2.waitForCompletion(true);
		System.out.println("finish job2");
	}
}

