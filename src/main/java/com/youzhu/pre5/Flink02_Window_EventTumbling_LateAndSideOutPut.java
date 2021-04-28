package com.youzhu.pre5;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class Flink02_Window_EventTumbling_LateAndSideOutPut {

    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //读取端口数据 并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> sensorSingleOutputStreamOperator = env.socketTextStream("localhost", 9999)
                .map(x -> {
                    String[] split = x.split(",");
                    return new WaterSensor(split[0], Long.parseLong(split[1]), Integer.parseInt(split[2]));
                });

        //提取数据中的时间戳作为Watermark字段  泛型方法写在方法前

        //自增的   乱序的情况下  会丢失数据 不延迟的情况
      /*  WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy.<WaterSensor>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                //毫秒
                return element.getTs() * 1000L;
            }
        });

       */
        //watermark  flink  保证数据不丢失  是 watermark + 允许窗口迟到 + 侧输出流   flink开窗的操作  类似实现了 lamada架构
        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = WatermarkStrategy
                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs() * 1000L;
            }
        });

        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = sensorSingleOutputStreamOperator.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        //按照id分组
        KeyedStream<WaterSensor, String> keyedStream = waterSensorSingleOutputStreamOperator.keyBy(WaterSensor::getId);

        //开窗
        WindowedStream<WaterSensor, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //允许窗口迟到  在允许窗口迟到数据是来一条处理一条
               .allowedLateness(Time.seconds(2))
                //侧输出流也是来一条处理一条
                .sideOutputLateData(new OutputTag<>("side"));

        //计算总和
        SingleOutputStreamOperator<WaterSensor> vc = window.sum("vc");
       //获取侧输出流
        DataStream<Object> side = vc.getSideOutput(new OutputTag<>("side"));
        vc.print();
        side.print("side");

        env.execute();



    }

}
