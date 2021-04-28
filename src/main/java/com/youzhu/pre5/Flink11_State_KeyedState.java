package com.youzhu.pre5;

import com.youzhu.bean.WaterSensor;
import lombok.SneakyThrows;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class Flink11_State_KeyedState {

    @SneakyThrows
    public static void main(String[] args) {

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

        //状态的使用
        waterSensorStringKeyedStream.process(new MyStateProcessFunc());

        //打印

        //执行
        env.execute();

    }


    public static class MyStateProcessFunc extends KeyedProcessFunction<String,WaterSensor,WaterSensor>{

        //a定义状态
        private ValueState<Long> valueState;
        private ListState<Long> listState;
        private MapState<String,Long> mapState;
        private ReducingState<WaterSensor> reducingState;
        private AggregatingState<WaterSensor,WaterSensor> aggregateState;

        //b初始化  java中初始化必须在open方法中

        @Override
        public void open(Configuration parameters) throws Exception {
            valueState =  getRuntimeContext().getState(new ValueStateDescriptor<Long>("value-state",Long.class));
            listState = getRuntimeContext().getListState(new ListStateDescriptor<Long>("list-state",Long.class));
            mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Long>("map-state",String.class,Long.class));
            //reducingState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<WaterSensor>())
            //aggregateFunction = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<WaterSensor, Object, WaterSensor>());

        }

        @Override
        public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {
            /* CRUD-BOY*/
           //c 状态使用

            //c.1 Value状态
            Long value1 = valueState.value();
            valueState.update(122L);
            valueState.clear();

            //c.2 ListState
            Iterable<Long> longs = listState.get();
            listState.add(112L);
            listState.clear();
            listState.update(new ArrayList<>());

            //c.3 Map
            Iterator<Map.Entry<String, Long>> iterator = mapState.iterator();
            Long aLong = mapState.get(" ");
            boolean contains = mapState.contains("");
            mapState.put("",122L);
            mapState.putAll(new HashMap<>());
            mapState.remove("");
            mapState.clear();

            //c.4 Reduce
            WaterSensor waterSensor = reducingState.get();
            reducingState.add(new WaterSensor());
            reducingState.clear();

            //c.5 Agg状态
            aggregateState.add(value);
            aggregateState.get();
            aggregateState.clear();

        }
    }
}
