package com.sparrowrecsys.online.recprocess;

import com.sparrowrecsys.online.datamanager.DataManager;
import com.sparrowrecsys.online.datamanager.Movie;
import com.sparrowrecsys.online.datamanager.User;

import java.util.*;

/**
 * Recommendation process of similar movies
 */

public class SimilarMovieProcess {

    /**
     * get recommendation movie list
     * @param movieId input movie id
     * @param size  size of similar items
     * @param model model used for calculating similarity
     * @return  list of similar movies
     */
    public static List<Movie> getRecList(int movieId, int size, String model){
        Movie movie = DataManager.getInstance().getMovieById(movieId);
        if (null == movie){
            return new ArrayList<>();
        }
        // 相似电影推荐，先进行召回，然后排序。
        // 召回: 根据当前电影所属的genres，每个genre按照评分或releasetime排序取前一百部
        List<Movie> candidates = candidateGenerator(movie);
        List<Movie> rankedList = ranker(movie, candidates, model);

        if (rankedList.size() > size){
            return rankedList.subList(0, size);
        }
        return rankedList;
    }

    /**
     * generate candidates for similar movies recommendation
     * @param movie input movie object
     * @return  movie candidates
     */
    public static List<Movie> candidateGenerator(Movie movie){
        HashMap<Integer, Movie> candidateMap = new HashMap<>();
        for (String genre : movie.getGenres()){
            List<Movie> oneCandidates = DataManager.getInstance().getMoviesByGenre(genre, 100, "rating");
            for (Movie candidate : oneCandidates){
                candidateMap.put(candidate.getMovieId(), candidate);
            }
        }
        candidateMap.remove(movie.getMovieId());
        return new ArrayList<>(candidateMap.values());
    }

    /**
     * multiple-retrieval candidate generation method
     * @param movie input movie object
     * @return movie candidates
     */
    public static List<Movie> multipleRetrievalCandidates(Movie movie){
        if (null == movie){
            return null;
        }

        HashSet<String> genres = new HashSet<>(movie.getGenres());

        HashMap<Integer, Movie> candidateMap = new HashMap<>();
        for (String genre : genres){
            List<Movie> oneCandidates = DataManager.getInstance().getMoviesByGenre(genre, 20, "rating");
            for (Movie candidate : oneCandidates){
                candidateMap.put(candidate.getMovieId(), candidate);
            }
        }

        List<Movie> highRatingCandidates = DataManager.getInstance().getMovies(100, "rating");
        for (Movie candidate : highRatingCandidates){
            candidateMap.put(candidate.getMovieId(), candidate);
        }

        List<Movie> latestCandidates = DataManager.getInstance().getMovies(100, "releaseYear");
        for (Movie candidate : latestCandidates){
            candidateMap.put(candidate.getMovieId(), candidate);
        }

        candidateMap.remove(movie.getMovieId());
        return new ArrayList<>(candidateMap.values());
    }

    /**
     * embedding based candidate generation method
     * @param movie input movie
     * @param size  size of candidate pool
     * @return  movie candidates
     */
    public static List<Movie> retrievalCandidatesByEmbedding(Movie movie, int size){
        if (null == movie || null == movie.getEmb()){
            return null;
        }

        List<Movie> allCandidates = DataManager.getInstance().getMovies(10000, "rating");
        HashMap<Movie,Double> movieScoreMap = new HashMap<>();
        for (Movie candidate : allCandidates){
            double similarity = calculateEmbSimilarScore(movie, candidate);
            movieScoreMap.put(candidate, similarity);
        }

        List<Map.Entry<Movie,Double>> movieScoreList = new ArrayList<>(movieScoreMap.entrySet());
        movieScoreList.sort(Map.Entry.comparingByValue());

        List<Movie> candidates = new ArrayList<>();
        for (Map.Entry<Movie,Double> movieScoreEntry : movieScoreList){
            candidates.add(movieScoreEntry.getKey());
        }

        return candidates.subList(0, Math.min(candidates.size(), size));
    }

    /**
     * 根据用户的embedding和电影的embedding进行相似召回
     */
//    public static List<Movie> retrievalCandidatesByEmbedding(User user){
//        if (null == user){
//            return null;
//        }
//        //获取用户embedding向量
//        double[] userEmbedding = DataManager.getInstance().getUserEmbedding(user.getUserId(), "item2vec");
//        if (null == userEmbedding){
//            return null;
//        }
//        //获取所有影片候选集(这里取评分排名前10000的影片作为全部候选集)
//        List<Movie> allCandidates = DataManager.getInstance().getMovies(10000, "rating");
//        HashMap<Movie,Double> movieScoreMap = new HashMap<>();
//        //逐一获取电影embedding，并计算与用户embedding的相似度
//        for (Movie candidate : allCandidates){
//            double[] itemEmbedding = DataManager.getInstance().getItemEmbedding(candidate.getMovieId(), "item2vec");
//            double similarity = calculateEmbeddingSimilarity(userEmbedding, itemEmbedding);
//            movieScoreMap.put(candidate, similarity);
//        }
//
//        List<Map.Entry<Movie,Double>> movieScoreList = new ArrayList<>(movieScoreMap.entrySet());
//        //按照用户-电影embedding相似度进行候选电影集排序
//        movieScoreList.sort(Map.Entry.comparingByValue());
//
//
//        //生成并返回最终的候选集
//        List<Movie> candidates = new ArrayList<>();
//        for (Map.Entry<Movie,Double> movieScoreEntry : movieScoreList){
//            candidates.add(movieScoreEntry.getKey());
//        }
//        return candidates.subList(0, Math.min(candidates.size(), size));
//    }

    /**
     * rank candidates
     * @param movie    input movie
     * @param candidates    movie candidates
     * @param model     model name used for ranking
     * @return  ranked movie list
     */
    public static List<Movie> ranker(Movie movie, List<Movie> candidates, String model){
        HashMap<Movie, Double> candidateScoreMap = new HashMap<>();
        for (Movie candidate : candidates){
            double similarity;
            switch (model){
                case "emb":
                    similarity = calculateEmbSimilarScore(movie, candidate);
                    break;
                default:
                    // 根据影片类型和评分的相似性计算
                    similarity = calculateSimilarScore(movie, candidate);
            }
            candidateScoreMap.put(candidate, similarity);
        }
        List<Movie> rankedList = new ArrayList<>();
        candidateScoreMap.entrySet().stream().sorted(Map.Entry.comparingByValue(Comparator.reverseOrder())).forEach(m -> rankedList.add(m.getKey()));
        return rankedList;
    }

    /**
     * function to calculate similarity score
     * @param movie     input movie
     * @param candidate candidate movie
     * @return  similarity score
     */
    public static double calculateSimilarScore(Movie movie, Movie candidate){
        int sameGenreCount = 0;
        for (String genre : movie.getGenres()){
            if (candidate.getGenres().contains(genre)){
                sameGenreCount++;
            }
        }
        double genreSimilarity = (double)sameGenreCount / (movie.getGenres().size() + candidate.getGenres().size()) / 2;
        double ratingScore = candidate.getAverageRating() / 5;

        double similarityWeight = 0.7;
        double ratingScoreWeight = 0.3;

        return genreSimilarity * similarityWeight + ratingScore * ratingScoreWeight;
    }

    /**
     * function to calculate similarity score based on embedding
     * @param movie     input movie
     * @param candidate candidate movie
     * @return  similarity score
     */
    public static double calculateEmbSimilarScore(Movie movie, Movie candidate){
        if (null == movie || null == candidate){
            return -1;
        }
        return movie.getEmb().calculateSimilarity(candidate.getEmb());
    }
}
