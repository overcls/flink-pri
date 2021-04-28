package com.youzhu.demo;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink01_DistinctByWordCount {
    /*
    tips 如何不用sum算子实现 wordcount 即使用 count++ 考虑到并行度为1 就有问题
                                        借用hashmap可以得到解决
     */

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9999);

        socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] wordds = value.split(" ");
                for (String wordd : wordds) {
                    out.collect(new Tuple2<>(wordd,1));
                }
            }
        })
                .keyBy(0)
                .sum(1)
                .filter(new FilterFunction<Tuple2<String, Integer>>() {
                    @Override
                    public boolean filter(Tuple2<String, Integer> value) throws Exception {
                        return value.f1 == 1;
                    }
                }).map(new MapFunction<Tuple2<String, Integer>, String>() {
            @Override
            public String map(Tuple2<String, Integer> value) throws Exception {
                return value.f0;
            }
        })
                .print();
    env.execute();
    }


}
