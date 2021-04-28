package com.youzhu.pre3;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink05_Transform_Process {
//process 可进行状态编程
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        DataStreamSource<String> socketTextStream = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<String> process = socketTextStream.process(new ProcessFlatMapFunc());

        //使用process实现map
        SingleOutputStreamOperator<Tuple2<String, Integer>> process1 = process.process(new ProcessMap());

        //分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream = process1.keyBy(data -> data.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedStream.sum(1);

        result.print();

        env.execute(" ");


    }
    public  static class ProcessFlatMapFunc extends ProcessFunction<String,String>{
        //生命周期
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
        }

        @Override
        public void processElement(String value, Context ctx, Collector<String> out) throws Exception {


            //运行上下文
            RuntimeContext runtimeContext = getRuntimeContext();
            String[] split = value.split(" ");
            for (String s : split) {
                out.collect(s);
            }
            //定时器
            TimerService timerService = ctx.timerService();
            timerService.registerEventTimeTimer(123);

            //获取处理数据的时间
            timerService.currentProcessingTime();  //处理时间
            timerService.currentWatermark();       //事件时间

            //侧输出流
            //ctx.output();
        }
    }
    public static class ProcessMap extends ProcessFunction<String, Tuple2<String,Integer>>{
        @Override
        public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            out.collect(new Tuple2<>(value,1));
        }
    }

}
