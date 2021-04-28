package com.youzhu.pre6;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Iterator;

public class Flink06_State_ListState_OP {


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

       //统计元素的个数  列表状态 Operator状态 都可以转换为别的去使用很少使用到这个
        sensorSingleOutputStreamOperator.map(new MyMapFunc()).print();



        env.execute();

    }
    public static class MyMapFunc implements MapFunction<WaterSensor,Integer>, CheckpointedFunction{

        //定义状态
        private ListState<Integer> listState;
        //Integer 没有默认值
        private Integer count = 0 ;

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

            listState = context
                    //不能直接使用上下文 必须指明是一个operator
                    .getOperatorStateStore()
                    .getListState(new ListStateDescriptor<Integer>("state",Integer.class));

            //将count保存到状态里
            Iterator<Integer> iterator = listState.get().iterator();

            while(iterator.hasNext()){
                count += iterator.next();
            }

        }


        @Override
        public Integer map(WaterSensor value) throws Exception {

            count++;
            return count;

        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

            listState.clear();
            listState.add(count);
        }


    }
}
