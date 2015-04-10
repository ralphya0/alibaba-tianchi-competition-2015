package me.ralphya0.alibaba_tianchi_competition_2015;

import java.io.Serializable;
import java.util.ArrayList;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

class RawRecord implements Serializable {
    String user_id;
    String item_id;
    String behavior_type;
    String user_geohash;
    String item_category;
    //自
    String time ;
    
}

//提取用户-商品交互特征
public class FeatureExtraction {
    //统计特征
    final static String[] tongji = {"1_hour_before_1","1_hour_before_2","1_hour_before_3","1_hour_before_4",
        "6_hours_before_1","6_hours_before_2","6_hours_before_3","6_hours_before_4",
        "24_hours_before_1","24_hours_before_2","24_hours_before_3","24_hours_before_4",
        "72_hours_before_1","72_hours_before_2","72_hours_before_3","72_hours_before_4",
        "7_days_before_1","7_days_before_2","7_days_before_3","7_days_before_4",
        "30_days_before_1","30_days_before_2","30_days_before_3","30_days_before_4",
        "last_time_last_moment_1","last_time_last_moment_2","last_time_last_moment_3","last_time_last_moment_4",
        "first_time_last_time_1","first_time_last_time_2","first_time_last_time_3","first_time_last_time_4",
        };
    //比率特征
    final static String[] bilv = {"brand_1_to_total_1","brand_234_to_total_1","brand_4_to_total_4"
        ,"brand_1234_to_total_user_1234","brand_1_24_hours_to_total_1","brand_1_12_hours_to_total_1"
        ,"brand_1_3_hours_to_total_1","brand_1_1_hours_to_total_1","brand_1234_day_to_total_user_1234_day",
        "brand_4_day_to_total_user_4_day"};
    //转化特征
    final static String[] zhuanhua = {"brand_4_to_brand_1",
        "brand_4_to_brand_2","brand_4_to_brand_3","brand_3_to_brand_1","brand_2_to_brand_1"};
    
    
    public static void main(String[] args) {
        //需要用户指定的运行参数
        //spark standalone master
        String master = "null";
        //输入文件位置
        String input = "null";
        //训练数据分割日期
        String splite_date = "null";
        //将原始训练数据按照日期拆分成独立文件
        String file_splite = "";
        if(args != null && args.length > 0){
            master = args[0].trim();
            input = args[1].trim();
            splite_date = args[2].trim();
            file_splite = args[3].trim();
        }
        
        SparkConf conf = new SparkConf().setAppName("tianchi-feature-extraction").setMaster(master);
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> lines = sc.textFile(input,3);
        
        //每条记录的主键是user_id和item_id!!!
        //维护若干个数据结构来指示已经处理过的user_id+item_id的组合以避免重复计算,同时要为特征计算提供便利
        //各项特征都需要完整扫描训练数据
        //扫描过程中将同一用户的信息归并到一起然后输出到文件中(方便特征统计!!!),同时将原始数据按照日期拆分成独立文件(作为独立小功能)
        //用户信息合并: Map<String,Map<String,Map<Integer,Map<Integer,Integer>>>> user_id, item_id,hours,action,count.其中hour的处理是关键
        //计算过程中涉及时间的操作一律统一以小时为计量单位
        
        //记得去除文件头!
        JavaPairRDD<String,RawRecord> pairs = lines.mapToPair(new PairFunction<String,String,RawRecord>(){

            public Tuple2<String, RawRecord> call(String arg0) throws Exception {
                if(arg0 != null && arg0.length() > 0){
                    String[] fields = arg0.split(",");
                    if(fields != null){
                        //以user_id为主键
                        String key = null;
                        RawRecord record = new RawRecord();
                        if(fields[0] != null){
                            key = fields[0];
                        }
                        if(fields[1] != null){
                            record.item_id = fields[1];
                        }
                        if(fields[2] != null){
                            record.behavior_type = fields[2];
                        }
                        if(fields[3] != null){
                            record.user_geohash = fields[3];
                        }
                        if(fields[4] != null){
                            record.item_category = fields[4];
                        }
                        if(fields[5] != null){
                            record.time = fields[5];
                        }
                        
                        return new Tuple2<String,RawRecord>(key,record);
                    }
                }
                return null;
            }
        });
        
        JavaPairRDD<String,Iterable<RawRecord>> userid_grouped = pairs.groupByKey();
        
        //hehe, 忽然发现把records以user_id聚合之后可以直接计算各种特征啦,各种复杂的数据结构可以省略掉 : )
        
    }

}
