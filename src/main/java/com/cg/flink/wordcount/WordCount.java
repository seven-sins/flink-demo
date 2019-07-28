package com.cg.flink.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {

	public static void main(String[] args) throws Exception {
		// 构建环境
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		// 获取数据集
		DataSet<String> text = env.fromElements("Who's there?", "I think I hear them. Stand, ho! Who's there?");
		
		// 切分数据
		DataSet<Tuple2<String, Integer>> wordCounts = text.flatMap(new LineSplitter()).groupBy(0).sum(1);
		
		wordCounts.print();
	}
	
	public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>>{
		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
			for(String word: line.split(" ")) {
				out.collect(new Tuple2<String, Integer>(word, 1));
			}
		}
	}
}
