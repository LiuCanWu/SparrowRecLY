package com.sparrowrecsys.nearline.flink;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import com.sparrowrecsys.nearline.flink.Rating;
import redis.clients.jedis.Jedis;

import javax.security.auth.login.Configuration;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collector;

//class Rating{
//    public String userId;
//    public String movieId;
//    public String rating;
//    public String timestamp;
//    public String latestMovieId;
//
//    public Rating(String line){
//        String[] lines = line.split(",");
//        this.userId = lines[0];
//        this.movieId = lines[1];
//        this.rating = lines[2];
//        this.timestamp = lines[3];
//        this.latestMovieId = lines[1];
//    }
//
//    @Override
//    public String toString() {
//        return this.userId + "," + this.movieId + "," + this.rating + "," + this.timestamp;
//    }
//}

public class RealTimeRecProcess {

    public void test() throws Exception {
        // set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment
                .getExecutionEnvironment();

        URL ratingResourcesPath = this.getClass().getResource("/webroot/sampledata/realtimeRatings.csv");

        // monitor directory, checking for new files
        TextInputFormat format = new TextInputFormat(
                new org.apache.flink.core.fs.Path(ratingResourcesPath.getPath()));

        DataStream<String> inputStream = env.readFile(
                format,
                ratingResourcesPath.getPath(),
                FileProcessingMode.PROCESS_CONTINUOUSLY,
                100);

        DataStream<Rating> ratingStream = inputStream.map(Rating::new);

        ratingStream.keyBy(rating -> rating.userId)
//                .timeWindow(Time.seconds(1))
//                .reduce(
//                        (ReduceFunction<Rating>) (rating, t1) -> {
//                            if (rating.timestamp.compareTo(t1.timestamp) > 0){
//                                return rating;
//                            }else{
//                                return t1;
//                            }
//                        }
//                )
                .addSink(new SinkFunction<Rating>() {
                    @Override
                    public void invoke(Rating value, Context context) {
                        Jedis jedis = null;
                        try {
                            jedis = new Jedis("localhost", 6379);
                            String redisKey = "real_record:" + value.userId;
                            String redisValue = value.movieId;
                            jedis.rpush(redisKey, redisValue);
                            jedis.expire(redisKey, 300);
//                            jedis.set(redisKey, redisValue);
                            System.out.println("real_user_record_userId:" + value.userId + "\tlatestMovieId:" + value.latestMovieId);
                            System.out.println("===>当前用户" + value.userId + "的实时观影记录: " + jedis.lrange(redisKey, 0, -1));
                        } finally {
                            if (jedis != null) {
                                jedis.close();
                            }
                        }
                    }
                });

//        ratingStream
//                .keyBy(rating -> rating.userId)
//                .timeWindow(Time.seconds(1))
//                .process(new ProcessWindowFunction<Rating, Rating, String, TimeWindow>() {
//
//                    @Override
//                    public void process(String s, ProcessWindowFunction<Rating, Rating, String, TimeWindow>.Context context, Iterable<Rating> iterable, org.apache.flink.util.Collector<Rating> collector) throws Exception {
//
//                    }
//
//                    // 用来保存每个用户的评分数据
//                    private ListState<Rating> ratingListState;
//
//                    @Override
//                    public void open(Configuration parameters) {
//                        ListStateDescriptor<Rating> descriptor = new ListStateDescriptor<>(
//                                "ratings",
//                                Rating.class
//                        );
//                        ratingListState = getRuntimeContext().getListState(descriptor);
//                    }
//
//                    @Override
//                    public void process(String userId, Context context, Iterable<Rating> elements, Collector<Rating> out) throws Exception {
//                        // 清空之前的评分列表
//                        ratingListState.clear();
//
//                        // 将当前窗口的评分数据加入状态
//                        for (Rating rating : elements) {
//                            ratingListState.add(rating);
//                        }
//
//                        // 通过用户 ID 读取评分列表
//                        List<Rating> ratings = new ArrayList<>();
//                        for (Rating rating : ratingListState.get()) {
//                            ratings.add(rating);
//                        }
//
//                        // 按时间戳排序，选择最新的两个评分
//                        ratings.sort(Comparator.comparing(r -> r.timestamp));
//                        Collections.reverse(ratings); // 反转以获得最新的在前
//
//                        // 输出最新的两个评分
//                        int count = 0;
//                        for (Rating rating : ratings) {
//                            if (count < 2) {
//                                out.collect(rating);
//                                count++;
//                            } else {
//                                break;
//                            }
//                        }
//                    }
//                })
//                .addSink(new SinkFunction<Rating>() {
//                    @Override
//                    public void invoke(Rating value, Context context) {
//                        System.out.println("userId:" + value.userId + "\tlatestMovieId:" + value.latestMovieId);
//                    }
//                    });
        env.execute();
    }

    public static void main(String[] args) throws Exception {
        new RealTimeRecProcess().test();
    }
}
