package com.youzhu.pre12;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class FlinkSQL04_Function_UDTF {

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
       tableEnv.createTemporarySystemFunction("split",Split.class);

        //TableAPI     同时又TableAPI和Sql执行会进入阻塞状态
/*        table
                .joinLateral(call("split",$("id")))
                .select($("id"),$("idSplit"))
                .execute()
                .print();*/

        //SQL
        tableEnv.sqlQuery("select id , word from " +table + ", lateral table(split(id))").execute().print();


        //执行任务
        env.execute();
    }
    //这里的泛型是out的泛型
    @FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
    public static class Split extends TableFunction<Row>{

        public void eval(String value){
            String[] split = value.split("_");
            for (String s : split) {
                collect(Row.of(s));
            }
        }

    }
}
