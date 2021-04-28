package com.youzhu.pre4;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Flink10_Window_TimeSession {
    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取端口数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9999);

        //压平为元组
        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] s = value.split(" ");
                for (String s1 : s) {

                    out.collect(new Tuple2<>(s1, 1));
                }
            }
        });

        //按照单词分组
        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = tuple2SingleOutputStreamOperator.keyBy(data -> data.f0);


        //开窗  会话窗口   会话间隔时间为5s
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = tuple2StringKeyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));

        //聚合计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = window.sum(1);

        //打印
        result.print();

        //执行
        env.execute();
    }
}
