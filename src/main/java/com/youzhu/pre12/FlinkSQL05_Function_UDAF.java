package com.youzhu.pre12;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class FlinkSQL05_Function_UDAF {

    public static void main(String[] args) throws Exception {

        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取端口数据并转换为JavaBean
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("localhost", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });

        //将流转换为动态表
        Table table = tableEnv.fromDataStream(waterSensorDS);

        /*
        面向官网编程
         */

        //不存在 rightLateraljoin

        //注册自定义函数并使用  每个人都可以使用
        tableEnv.createTemporarySystemFunction("split",MyAvg.class);

        //TableAPI     当聚合函数和上一次聚合结果相同时 不输出新值
        table.groupBy($(""))
                .select($("id"),call("MyAvg",$("vc")))
                .execute()
                .print();

        //SQL
        tableEnv.sqlQuery("select id ,MyAvg(vc) from " + table + " group by id").execute().print();


        //执行任务
        env.execute();
    }

    public static class MyAvg extends AggregateFunction<Double, SumCount> {

        @Override
        public SumCount createAccumulator() {
            return new SumCount();
        }

        public void accumulate(SumCount acc, Integer vc) {

            acc.setVcSum(acc.getVcSum() + vc);
            acc.setVcSum(acc.getCount() + 1);

        }

        @Override
        public Double getValue(SumCount accumulator) {
            return accumulator.getVcSum() * 1D / accumulator.getCount();
        }


    }
}
