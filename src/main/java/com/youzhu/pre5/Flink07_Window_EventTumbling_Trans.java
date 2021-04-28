package com.youzhu.pre5;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

public class Flink07_Window_EventTumbling_Trans {

    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        WatermarkStrategy<String> waterSensorWatermarkStrategy = WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<String>() {
            @Override
            public long extractTimestamp(String element, long recordTimestamp) {
                String[] split = element.split(",");
                return Long.parseLong(split[1]) * 1000L;
            }
        });

        //读取端口数据 并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> sensorSingleOutputStreamOperator = env.socketTextStream("localhost", 9999)
                .assignTimestampsAndWatermarks(waterSensorWatermarkStrategy)
                .map(x -> {
                    String[] split = x.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        //提取数据中的时间戳作为Watermark字段  泛型方法写在方法前

        //watermark


       // SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = sensorSingleOutputStreamOperator.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        //按照id分组
        KeyedStream<WaterSensor, String> keyedStream = sensorSingleOutputStreamOperator.keyBy(WaterSensor::getId);

        //开窗
        WindowedStream<WaterSensor, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));
                //允许窗口迟到
               // .allowedLateness(Time.seconds(3));

        //计算总和
        SingleOutputStreamOperator<WaterSensor> vc = window.sum("vc");

        vc.print();

        env.execute();


        /*
        WarterMark传递
        1.使用广播的方式传输的
        2.某个并行度中的Watermark值取决于前面所有并行度的最小WaterMark值
        3.当WaterMark值没有增长的时候,不会向下游传递,注意:生成不变
         */


    }

}
