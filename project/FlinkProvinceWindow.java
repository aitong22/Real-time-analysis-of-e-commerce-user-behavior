package com.edu.neusoft.project;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.concurrent.TimeUnit;

/**
 * Flink任务4-各省份地区商品相关统计：
 *           每10s计算一次这段时间内该地区的点击、管住、售卖商品数量
 *           滚动窗口
 */
public class FlinkProvinceWindow {

    public static void main(String[] args) throws Exception {

        //1.初始化env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

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
        String groupId = "realtime_province_group";
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


        //3.transform
        SingleOutputStreamOperator<Tuple5<String, String, Integer, Integer, Integer>> stream = source.flatMap(new FlatMapFunction<String, Tuple5<String, String, Integer, Integer, Integer>>() {

            @Override
            public void flatMap(String json, Collector<Tuple5<String, String, Integer, Integer, Integer>> out) throws Exception {

                JSONObject jo = JSON.parseObject(json);
                Integer action = jo.getInteger("action");
                String province = jo.getString("province");

                Integer clickCount = 0; //点击数
                Integer buyCount = 0;  //购买数
                Integer focusCount = 0; //关注数
                switch (action) {
                    case 0:
                        clickCount++;
                        break;
                    case 2:
                        buyCount++;
                        break;
                    case 3:
                        focusCount++;
                        break;
                    default:
                        break;
                }

                if (province.endsWith("市")) {
                    //直辖市去掉“市”
                    province = province.replaceAll("市", "");
                }
                String provinceCode = AddressUtils.getIso3316CodeByProvince(province);
                out.collect(new Tuple5<>(province, provinceCode, clickCount, focusCount, buyCount));
            }
        })
        .keyBy(t -> t.f0)
        .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
        .reduce(new ReduceFunction<Tuple5<String, String, Integer, Integer, Integer>>() {
               @Override
               public Tuple5<String, String, Integer, Integer, Integer> reduce(Tuple5<String, String, Integer, Integer, Integer> t1, Tuple5<String, String, Integer, Integer, Integer> t2) throws Exception {
                    return new Tuple5<>(t1.f0, t1.f1, t1.f2+t2.f2, t1.f3+t2.f3, t1.f4+t2.f4);
               }
        });

        //4.sink
        //Sink到控制台
        stream.print();

        //Sink到MySQL
        String drivername = "com.mysql.jdbc.Driver";
        String dburl = "jdbc:mysql://ubuntu/realtime?useSSL=false";
        String username = "root";
        String password = "hadoop";
        String sql = "insert into realtime_province_window(province, province_iso_3316_code, click_count, focus_count, buy_count, `time`) values (?,?,?,?,?,?)";

        SinkFunction<Tuple5<String, String, Integer, Integer ,Integer>> jdbcSink = JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<Tuple5<String, String, Integer, Integer ,Integer>>() {
                    @Override
                    public void accept(PreparedStatement pstmt, Tuple5<String, String, Integer, Integer ,Integer> t) throws SQLException {
                        pstmt.setString(1,t.f0);
                        pstmt.setString(2,t.f1);
                        pstmt.setInt(3,t.f2);
                        pstmt.setInt(4,t.f3);
                        pstmt.setInt(5,t.f4);
                        pstmt.setTimestamp(6,new Timestamp(System.currentTimeMillis()));
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
        stream.addSink(jdbcSink);

        //5.启动env
        env.execute("FlinkProvinceWindow");
    }
}

