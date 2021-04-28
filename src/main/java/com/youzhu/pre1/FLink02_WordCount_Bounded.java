package com.youzhu.pre1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FLink02_WordCount_Bounded {

    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度  默认是key的hash值%核数
        env.setParallelism(1);

        //读取文件创建流
        DataStreamSource<String> input = env.readTextFile("input");

        //压平将单词转换为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToLine = input.flatMap(new LineToTupleFlatMapFunc());

        //分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordToLine.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        });

        //按照key做聚合操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        //打印结果
        result.print();

        //启动任务
        env.execute("FLink02_WordCount_Bounded");

    }
    //将一行数据变为元组
    public static class LineToTupleFlatMapFunc implements FlatMapFunction<String, Tuple2<String,Integer>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {

            //按空格切分数据
            String[] words = value.split(" ");
            //遍历写出
            for (String word : words) {
                out.collect(new Tuple2<>(word,1));
            }
        }
    }

}
