package com.youzhu.pre4;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink01_RunMode_Batch {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //指定为批处理模式   小bug  如果只出现一次  最终就不会出现
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStreamSource<String> stringDataStreamSource = env.readTextFile("input/sensor.txt");

        SingleOutputStreamOperator<Tuple2<String, Integer>> outputStreamOperator = stringDataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] s = value.split(" ");
                for (String s1 : s) {
                    out.collect(new Tuple2<>(s1, 1));
                }
            }
        });
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = outputStreamOperator.keyBy(x -> x.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyedStream.sum(1);

        sum.print();

        env.execute();


    }
}
