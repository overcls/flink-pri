package com.youzhu.pre12;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.call;

public class FlinkSQL03_Function_UDF {

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

        //不注册函数直接使用  不可复用
        table.select(call(MyLength.class,$("id"))).execute().print();

        //注册自定义函数并使用  每个人都可以使用
        tableEnv.createTemporarySystemFunction("Mylen",MyLength.class);

        //TableAPI     同时又TableAPI和Sql执行会进入阻塞状态
        table.select($("id"),call("Mylen"),$("id")).execute().print();

        //SQL
        tableEnv.sqlQuery("select id , Mylen(id) from " +table).execute().print();


        //执行任务
        env.execute();
    }

    public static class MyLength extends ScalarFunction{

        //以下方法并非实现 而是需要自己去写
        public int eval(String value){
            return value.length();
        }

    }
}
