package com.youzhu.pre6;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink01_State_ValueState {


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

        //使用richfunction实现状态编程实现水位线跳变报警需求   与process 比较没有定时器  多数使用process
        waterSensorStringKeyedStream.flatMap(new RichFlatMapFunction<WaterSensor, String>() {

            //定义状态
            private ValueState<Integer> vcState;

            //在open方法中 对状态进行初始化
            @Override
            public void open(Configuration parameters) throws Exception {
                vcState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("vc-state",Integer.class));
            }

            @Override
            public void flatMap(WaterSensor value, Collector<String> out) throws Exception {

                //获取状态中的数据
                Integer lastVc = vcState.value();

                //更新状态
                vcState.update(value.getVc());

                //当上一次水位线不为null 并且出现跳变的时候 进行报警
                if (lastVc !=null && Math.abs(lastVc-value.getVc()) >10){

                    out.collect(value.getId()+"出现水位线跳变!!");

                }

            }
        }).print();

        env.execute();

    }
}
