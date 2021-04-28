package com.youzhu.pre5;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class Flink06_Window_CustomerPunt {

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


        WatermarkStrategy<WaterSensor> waterSensorWatermarkStrategy = new WatermarkStrategy<WaterSensor>() {

            @Override
            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
                return new MyPunt(2000L);
            }
        }.withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs() * 1000L;
            }
        });


        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperato
                = sensorSingleOutputStreamOperator.assignTimestampsAndWatermarks(waterSensorWatermarkStrategy);

        //按照id分组
        KeyedStream<WaterSensor, String> keyedStream = waterSensorSingleOutputStreamOperato.keyBy(WaterSensor::getId);

        //开窗
        WindowedStream<WaterSensor, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));
                //允许窗口迟到
               // .allowedLateness(Time.seconds(3));

        //计算总和
        SingleOutputStreamOperator<WaterSensor> vc = window.sum("vc");

        vc.print();

        //修改watermarkinterval
        env.getConfig().setAutoWatermarkInterval(500);
        env.execute();


    }
    //自定义周期watermark生成器   周期性调用watermark  watermarkinterval是可以修改的 默认200 ms  生成一次
    public static class  MyPunt implements  WatermarkGenerator<WaterSensor>{

        private Long maxTs;
        private Long maxDelay;

        public MyPunt(Long maxDelay) {
            this.maxDelay = maxDelay;
            //防止超过阈值 故 +1
            this.maxTs=Long.MIN_VALUE+maxDelay + 1;
        }


        //当数据来的时候调用
        @Override
        public void onEvent(WaterSensor event, long eventTimestamp, WatermarkOutput output) {
            maxTs = Math.max(eventTimestamp,maxTs);
            output.emitWatermark(new Watermark(maxTs-maxDelay));
        }

        //周期性调用
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
        }
    }

}
