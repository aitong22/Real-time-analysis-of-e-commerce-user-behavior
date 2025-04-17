package com.edu.neusoft.project;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

/**
 * Flink任务2-商品总体统计：
 *      计算活动开始后商品的点击数、销售数、关注数
 */
public class FlinkItemTotal {

    public static void main(String[] args) throws Exception {

        //1.初始化env、TableEnv
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //设置状态后端
        env.setStateBackend(new HashMapStateBackend());

        //===========Checkpoint参数设置====
        //===========类型1:必须参数=============
        //设置Checkpoint的时间间隔为1s做一次Checkpoint
        env.enableCheckpointing(1000);
        //设置快照的持久化存储位置
        if (SystemUtils.IS_OS_WINDOWS) {
            env.getCheckpointConfig().setCheckpointStorage("file:///D:/exam/ckp");
        } else {
            env.getCheckpointConfig().setCheckpointStorage("hdfs://ubuntu:9000/user/flink/realtime/exam/ckp");
        }
        //===========类型2:建议参数===========
        //设置两个Checkpoint 之间最少等待时间,如设置Checkpoint之间最少是要等 500ms(为了避免每隔1000ms做一次Checkpoint的时候,前一次太慢和后一次重叠到一起去了)
        //如:高速公路上,每隔1s关口放行一辆车,但是规定了两车之前的最小车距为500m
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);//默认是0
        //设置如果在做Checkpoint过程中出现错误，是否让整体任务失败
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(20);//默认值为0，表示不容忍任何检查点失败
        //设置是否清理检查点,表示 Cancel 时是否需要保留当前的 Checkpoint，默认 Checkpoint会在作业被Cancel时被删除
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //===========类型3:直接使用默认的即可===============
        //设置checkpoint的执行模式为EXACTLY_ONCE(默认)
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置checkpoint的超时时间,如果 Checkpoint在10min内尚未完成说明该次Checkpoint失败,则丢弃。
        env.getCheckpointConfig().setCheckpointTimeout(60000);//默认10分钟
        //设置同一时间有多少个checkpoint可以同时执行，配置了上面setMinPauseBetweenCheckpoints(500)，该项就没有意义了
        //env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);//默认为1
        //开启unaligned Checkpoint机制。
        env.getCheckpointConfig().enableUnalignedCheckpoints();

        //=================配置重启策略========
        //固定延迟重启--开发中常用 最多重启3次（重启时间间隔为5s），超过3次则失败
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, org.apache.flink.api.common.time.Time.of(5, TimeUnit.SECONDS)));



        //2.source（Kafka Source）
        String topic = "realtime-data";
        String groupId = "realtime_item_group";
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                //设置bootstrap.servers
                .setBootstrapServers("ubuntu:9092")
                //设置订阅的主题
                .setTopics(topic)
                //设置Conumer Group ID
                .setGroupId(groupId)
                //起始消费位移的指定
                //  OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST) 消费起始位移选择之前所提交的偏移量（如果没有，则重置为LATEST）
                //  OffsetsInitializer.earliest() 消费起始位移直接选择为“最早”
                //  OffsetsInitializer.latest()  消费起始位移直接选择为“最新”
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                //设置Value数据的反序列化器
                .setValueOnlyDeserializer(new SimpleStringSchema())
                //开启Kafka底层消费者的自动位移提交机制
                //   它会把最新的消费位移提交到kafka的consumer_offsets中
                //   就算把自动位移提交机制开启，KafkaSource依然不依赖自动位移提交机制
                //  （宕机重启时，优先从flink自己的状态中去获取偏移量<更可靠>）
                .setProperty("auto.offset.commit", "true")
                .build();

        DataStreamSource<String> source = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka-source");


        //3.transform:
        //3.1 DataStream[String] -> DataStream[UserBehaviorPojo]
        SingleOutputStreamOperator<UserBehaviorPojo> ubStream = source.map(json -> JSON.parseObject(json, UserBehaviorPojo.class));
        //3.2 DataStream -> Table -> TempView
        Table ubTable = tableEnv.fromDataStream(ubStream, "itemId, action, catId, brandId, pt.proctime");
        tableEnv.createTemporaryView("user_behavior", ubTable);
        ubTable.printSchema();
        //3.3 Flink SQL
        String sql = "select " +
                "         itemId, " +
                "         action, " +
                "         max(catId) AS catId, " +
                "         max(brandId) AS brandId, " +
                "         if(action = '0', count(*), 0) AS item_click_count, " +
                "         if(action = '2', count(*), 0) AS item_sale_count, " +
                "         if(action = '3', count(*), 0) AS item_focus_count " +
                "     from user_behavior " +
                "     group by itemId, action, tumble(pt, interval '3' second)";
        Table resultTable = tableEnv.sqlQuery(sql);
        //3.4 Table -> DataStream
        DataStream<Row> rowStream = tableEnv.toChangelogStream(resultTable);

        //4.sink
        //Sink到控制台
        rowStream.print();

        //Sink到MySQL
        String drivername = "com.mysql.jdbc.Driver";
        String dburl = "jdbc:mysql://ubuntu/realtime";
        String username = "root";
        String password = "hadoop";
        String sinkSql = "insert into realtime_item_total(item_id,category_id,brand_id,item_click_count,item_sale_count,item_focus_count,`time`) values(?,?,?,?,?,?,?)";

        SinkFunction<Row> jdbcSink = JdbcSink.sink(
                sinkSql,
                new JdbcStatementBuilder<Row>() {
                    @Override
                    public void accept(PreparedStatement pstmt, Row row) throws SQLException {
                        String itemId = row.getField(0).toString();
                        String catId = row.getField(2).toString();
                        String brandId = row.getField(3).toString();
                        Integer itemClickCount = Integer.valueOf(row.getField(4).toString());
                        Integer itemSaleCount = Integer.valueOf(row.getField(5).toString());
                        Integer itemFocusCount = Integer.valueOf(row.getField(6).toString());
                        pstmt.setString(1, itemId);
                        pstmt.setString(2, catId);
                        pstmt.setString(3, brandId);
                        pstmt.setInt(4, itemClickCount);
                        pstmt.setInt(5, itemSaleCount);
                        pstmt.setInt(6, itemFocusCount);
                        pstmt.setTimestamp(7,new Timestamp(System.currentTimeMillis()));
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(100)
                        .withBatchIntervalMs(200)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(dburl)
                        .withUsername(username)
                        .withPassword(password)
                        .withDriverName(drivername)
                        .build()
        );
        rowStream.addSink(jdbcSink);


        //execute
        env.execute("FlinkItemTotal");
    }
}

