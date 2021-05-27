package com.youzhu.pre7;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class Flink01_practice_WordCount_Slide_SideOutPut {

    /*
    使用事件时间处理数据,从端口获取数据实现每隔5s计算最近30s的每个传感器发送水位线的次数,
    Watermark设置延迟2s,再迟到的数据放至侧输出流,通过观察结果说明什么样的数据会进入侧输出流
     */
    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置并行度为1更方便观察数据
        env.setParallelism(1);

        //读取端口数据并转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = env.socketTextStream("localhost", 9999)
                .map(data -> {
                    String[] split = data.split(",");
                    return new WaterSensor(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });

        //提取数据中的时间戳生成Watermark
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs() * 1000L;
            }
        });
        SingleOutputStreamOperator<WaterSensor> StreamOperator = waterSensorSingleOutputStreamOperator
                .assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        //转换为元组类型
        SingleOutputStreamOperator<Tuple2<String, Integer>> idToOneDS = StreamOperator.map(new MapFunction<WaterSensor, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(WaterSensor value) throws Exception {
                return new Tuple2<>(value.getId(), 1);
            }
        });

        //按照ID分组
        KeyedStream<Tuple2<String, Integer>, String> KeyedStream = idToOneDS.keyBy(data -> data.f0);

        //开窗.滑动窗口
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowedStream = KeyedStream
                .window(SlidingEventTimeWindows.of(Time.seconds(30), Time.seconds(5)))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<Tuple2<String,Integer>>("SideOutPut"){});

        //计算总和
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowedStream.sum(1);

        //打印结果
        result.print("Result");
        result.getSideOutput(new OutputTag<Tuple2<String, Integer>>("SideOutPut"){}).print("Side");

        //执行
        env.execute();
    }
}
