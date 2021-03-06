package com.youzhu.pre11;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.lit;

public class FlinkSQL07_TableAPI_GroupWindow_SessionWindow_processTime {


    public static void main(String[] args) throws Exception {

        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //读取端口数据并转换为JavaBean对象
        SingleOutputStreamOperator<WaterSensor> waterSensorDS = env.socketTextStream("localhost", 9999)
                .map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0],
                            Long.parseLong(split[1]),
                            Integer.parseInt(split[2]));
                });

        //将流转换为表并指定处理时间
        Table table = tableEnv.fromDataStream(waterSensorDS,
                $("id"),
                $("ts"),
                $("vc"),
                $("pt").proctime());
        //开滚动窗口计算wordcount
        Table result = table.window(Session.withGap(lit(5).seconds()).on($("pt")).as("sw"))
                .groupBy($("id"), $("sw"))
                .select($("id"), $("id").count());

        //将结果表转换为流进行输出  追加流原因:一个窗口内没有数据修改  所以追加流就可以 不用撤回流
        tableEnv.toAppendStream(result, Row.class).print();
        //执行任务
        env.execute();
    }
}
