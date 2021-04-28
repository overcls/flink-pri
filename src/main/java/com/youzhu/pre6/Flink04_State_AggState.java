package com.youzhu.pre6;

import com.youzhu.bean.AvgVc;
import com.youzhu.bean.WaterSensor;
import com.youzhu.bean.WaterSensor2;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink04_State_AggState {


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


        //使用状态编程方式实现平均水位   agg输入输出可不一样
        waterSensorStringKeyedStream.process(new KeyedProcessFunction<String, WaterSensor, WaterSensor2>() {

            //定义状态
            private AggregatingState<Integer,Double> aggregatingState;

            @Override
            public void open(Configuration parameters) throws Exception {
                aggregatingState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<Integer, AvgVc, Double>("agg-state", new AggregateFunction<Integer, AvgVc, Double>() {
                    @Override
                    public AvgVc createAccumulator() {
                        return new AvgVc(0,0);
                    }

                    @Override
                    public AvgVc add(Integer value, AvgVc accumulator) {
                        return new AvgVc(accumulator.getVcSum()+value,accumulator.getCount()+1);
                    }

                    @Override
                    public Double getResult(AvgVc accumulator) {
                        return accumulator.getVcSum() * 1D
                                /accumulator.getCount();
                    }

                    @Override
                    public AvgVc merge(AvgVc a, AvgVc b) {
                        return new AvgVc(a.getVcSum()+b.getVcSum(),a.getCount()+b.getCount());
                    }
                },AvgVc.class));
            }

            @Override
            public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor2> out) throws Exception {

                //将当前数据累加进状态
                aggregatingState.add(value.getVc());

                //取出状态中的数据
                Double avgVc = aggregatingState.get();

                //输出数据
                out.collect(new WaterSensor2(value.getId(),value.getTs(),avgVc));


            }
        }).print();

        //执行
        env.execute();

    }
}
