package com.youzhu.pre5;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink12_Process_Vclnrc_Bystate {

    public static void main(String[] args) throws Exception {

        //获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        //设置时间语义 1.12 以前默认是processtime   12 版本 默认时间语义  eventtime   也不是说默认为eventtime  注册提取时间器等操作 他会默认实现 这些操作
       // env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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

            //定义状态
            private ValueState<Integer> vcState;
            private ValueState<Long> tsState;


            //初始化状态


            @Override
            public void open(Configuration parameters) throws Exception {
                vcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>(" Vc-state",Integer.class,Integer.MIN_VALUE));
                tsState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("ts-state",Long.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {

                //取出状态数据
                Integer lastValue = vcState.value();
                Long timeTs = tsState.value();

                //取出当前数据的水位线
                Integer curVc = value.getVc();

                //当水位上升并且timeTs为null的时候
                if (curVc ==lastValue && timeTs ==null) {
                    //注册定时器
                    long ts = ctx.timerService().currentProcessingTime() + 10000L;
                    ctx.timerService().registerProcessingTimeTimer(ts);
                    tsState.update(ts);
                }else if (curVc <lastValue  && timeTs !=null){
                    //删除定时器
                    ctx.timerService().deleteProcessingTimeTimer(timeTs);
                    tsState.clear();
                }
                //更新上一次水位线的状态
                vcState.update(curVc);

                //输出数据
                out.collect(value);

            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<WaterSensor> out) throws Exception {

                ctx.output(new OutputTag<String>("sideOutPut"){},ctx.getCurrentKey()+"连续10s没有下降");
            }

        });

        result.print("主流");
        result.getSideOutput(new OutputTag<String>("sideOutPut"){}).print("SideOutPut");
        env.execute();


    }
}
