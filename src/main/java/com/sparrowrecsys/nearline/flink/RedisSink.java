package com.sparrowrecsys.nearline.flink;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import redis.clients.jedis.Jedis;
import com.sparrowrecsys.nearline.flink.Rating;

import javax.security.auth.login.Configuration;

public class RedisSink implements SinkFunction<Rating> {

    private transient Jedis jedis;
    private final String redisHost;
    private final int redisPort;

    public RedisSink(String redisHost, int redisPort) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
    }

//    @Override
//    public void open(Configuration parameters) throws Exception {
//        super.open(parameters);
//        jedis = new Jedis(redisHost, redisPort);
//    }

    @Override
    public void invoke(Rating value, Context context) {
        String redisKey = "user:" + value.userId + ":latest_movie";
        String redisValue = "movieId:" + value.movieId + ", timestamp:" + value.timestamp;
        jedis.set(redisKey, redisValue);
    }
//
//    @Override
//    public void close() throws Exception {
//        super.close();
//        if (jedis != null) {
//            jedis.close();
//        }
//    }
}

