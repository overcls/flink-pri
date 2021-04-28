package com.youzhu.pre5;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink10_Process_Vclnrc {

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

        //按照传感器ID分组
        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = sensorSingleOutputStreamOperator.keyBy(WaterSensor::getId);

        //使用ProcessFunction连续时间内水位线不下降,则报警,且将报警信息输出到侧输出流
        SingleOutputStreamOperator<WaterSensor> result = waterSensorStringKeyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {

            private Integer lastVc = Integer.MIN_VALUE;
            //用来删除注册定时器的时间   因为是if-else逻辑  时间保持一致  为了删除和注册是同一定时器
            private Long timerTs = Long.MIN_VALUE;


            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {

                System.out.println(lastVc);

                //取出水位线
                Integer vc = value.getVc();

                //将当前水位线遇上一次值进行比较
                if (vc >= lastVc && timerTs == Long.MIN_VALUE) {
                    //注册定时器
                    long l = ctx.timerService().currentProcessingTime() + 10000L;
                    System.out.println(l);
                    ctx.timerService().registerProcessingTimeTimer(l);

                    //更新上一次的水位线值,更新定时器的时间戳

                    timerTs = l;
                } else if (vc < lastVc) {

                    //删除定时器
                    ctx.timerService().deleteProcessingTimeTimer(timerTs);
                    System.out.println("删除定时器"+timerTs);

                    //重置
                    timerTs = Long.MIN_VALUE;
                }

                //输出数据
                out.collect(value);
                //更新值
                lastVc = vc;
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {
                ctx.output(new OutputTag<String>("sideOut") {
                }, ctx.getCurrentKey() + "连续10s水位线没有下降");
                //重置
                timerTs = Long.MIN_VALUE;
            }
        });

        result.print("主流");
        result.getSideOutput(new OutputTag<String>("sideOut"){}).print("SideOutPut");
        env.execute();


    }
}
