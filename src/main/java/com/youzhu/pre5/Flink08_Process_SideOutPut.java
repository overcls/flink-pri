package com.youzhu.pre5;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink08_Process_SideOutPut {

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

        //使用processFunction将数据分流
        SingleOutputStreamOperator<WaterSensor> result = sensorSingleOutputStreamOperator.process(new SplitProcessFunc());

        result.print("zhuliu");

        result.getSideOutput(new OutputTag<Tuple2<String,Integer>>("SideOut" ){}).print("side");

        env.execute(" ");

    }

    public  static class SplitProcessFunc extends ProcessFunction<WaterSensor,WaterSensor>{

        @Override
        public void processElement(WaterSensor value, Context ctx, Collector<WaterSensor> out) throws Exception {

            //取出水位线
            Integer vc = value.getVc();

            //根据水位线高低分流
            if (vc>=30) {
                //将数据输出至主流
                out.collect(value);
            }else{
                //将数据放入侧输出流
               ctx.output(new OutputTag<Tuple2<String,Integer>>("SideOut"){
                   },
                       new Tuple2<>(value.getId(),vc));

            }

        }
    }

}
