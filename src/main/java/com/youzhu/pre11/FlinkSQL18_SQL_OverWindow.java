package com.youzhu.pre11;

import com.youzhu.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL18_SQL_OverWindow {


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

        //SQL
/*        Table result = tableEnv.sqlQuery("select " +
                " id , " +
                " sum(vc) over(partition by id order by pt ) sum_vc ," +
                "count(id) over(partition by id order by pt ) ct " +
                "from " + table);*/

        //flink独有写法  开窗  最好注册表  否则会有空格的原因
        Table result = tableEnv.sqlQuery("select " +
                " id , " +
                " sum(vc) over w as sum_vc," +
                "count(id) over w  as ct " +
                "from " + table +
                " window as (partition by id order by pt rows betweeen 2 preceding and current row)");

        //在flink中 over中的内容必须是一致的 hive支持 over中内容不同   会报错
/*        Table result = tableEnv.sqlQuery("select " +
                " id , " +
                " sum(vc) over( partition by id order by pt ) sum_vc ," +
                "count(id) over(order by pt ) ct " +
                "from " + table);*/

        //将结果表转换为流进行输出  追加流原因:一个窗口内没有数据修改  所以追加流就可以 不用撤回流
        tableEnv.toAppendStream(result, Row.class).print();

        //直接输出表  表格形式打印  cool
  //      result.execute().print();

        //执行任务
        env.execute();
    }
}
