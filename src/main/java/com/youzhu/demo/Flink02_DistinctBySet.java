package com.youzhu.demo;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class Flink02_DistinctBySet {

    public static void main(String[] args) throws Exception {

        HashSet<Object> hashset = new HashSet<>();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /*
        并行度默认采用的是轮询策略
         */
        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<String> wordDS = socketTextStream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });
        /*
        加keyby的原因  公用同一个hashset
         */
        wordDS.keyBy(x->x)
                .filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                if (hashset.contains(value)){
                    return false;
                }else{
                    hashset.add(value);
                    return true;
                }
            }
        }).print();
        env.execute(" ");
    }
}
