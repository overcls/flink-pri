package com.youzhu.pre6;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink03_State_ReducingState {


    public static void main(String[] args) throws Exception {

        //ReducingState 要求输入输出类型一致 Aggregating 不要求输入输出类型一致

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

        //使用状态编程的方式实现累加传感器的水位线  输入数据和输出数据一样
        waterSensorStringKeyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {

            //定义状态
            private ReducingState<WaterSensor> reducingState;

            @Override
            public void open(Configuration parameters) throws Exception {
                reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<WaterSensor>("reduce", new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        return new WaterSensor(value1.getId(),value2.getTs(),value1.getVc()+value2.getVc());
                    }
                },WaterSensor.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {

                //将当前数据聚合进状态
                reducingState.add(value);

                //取出状态中的数据
                WaterSensor waterSensor = reducingState.get();

                out.collect(waterSensor);

            }
        }).print();

        //执行
        env.execute();

    }
}
