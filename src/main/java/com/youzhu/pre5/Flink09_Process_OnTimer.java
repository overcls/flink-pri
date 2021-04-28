package com.youzhu.pre5;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

public class Flink09_Process_OnTimer {


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

        //使用ProcessFunction的定时器功能 定时器只能在keystream中使用
        sensorSingleOutputStreamOperator.keyBy(WaterSensor::getId).process(new ProcessFunction<WaterSensor, WaterSensor>() {


            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
                //获取当前数据的处理时间
                long ts = ctx.timerService().currentProcessingTime();
                System.out.println(ts);

                //注册定时器
                ctx.timerService().registerProcessingTimeTimer(ts+5000L);

                out.collect(value);
            }
            //注册的定时器响起,触发动作
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {

                System.out.println("定时器触发"+timestamp);
            }
        }).print();

        env.execute();
    }
}
