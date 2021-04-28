package com.youzhu.pre6;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink05_State_MapState {


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


        //使用状态编程实现水位线的去重
        waterSensorStringKeyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor>() {

            //定义状态
            private MapState<Integer,String> mapState;


            @Override
            public void open(Configuration parameters) throws Exception {
                mapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, String>("map-state",Integer.class,String.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {

                //取出当前是数据的水位线
                Integer vc = value.getVc();

                //判断当前状态中 是否包含当前的水位值
                if (!mapState.contains(vc)){
                    out.collect(value);
                    mapState.put(vc,"aa");
                }

            }
        }).print();

        //执行
        env.execute();

    }
}
