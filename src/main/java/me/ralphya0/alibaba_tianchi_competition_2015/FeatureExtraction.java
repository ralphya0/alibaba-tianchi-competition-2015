package me.ralphya0.alibaba_tianchi_competition_2015;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.HashSet;
import java.util.Set;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;

import scala.Tuple2;

class InteractionRecord implements Serializable {
    String user_id;
    String item_id;
    String behavior_type;
    String user_geohash;
    String item_category;
    //自11.18日零时起的小时数
    long hour;
    String time;
    
}

//暂时不用ItemRecord
class ItemRecord implements Serializable{
    String item_id;
    String item_geohash;
    String item_category;
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
    
    //统计时间点
    final static long[] tongji_hour = {1, 3, 6, 12, 24, 72, 168, 720};
    
    //垂直商品列表
    final static Set<String> target_items = new HashSet<String>();
    //分割日期，如12-16 + 12-17 + 12-18(转换为小时)
    final static Map<String,Long> split_dates = new HashMap<String,Long>();
    
    public static void main(String[] args) throws IOException {
        //需要用户指定的运行参数
        //spark standalone master
        String master = "null";
        //输入文件位置
        String input = "null";
        //垂直商品文件
        String item_input = "null";
        //特征提取结果输出hdfs目录
        String hdfs_out = "null";
        //训练数据分割日期(输入12-16,则分割出12-16,12-17和12-18三个特征文件)
        String split_date = "null";
        //将原始训练数据按照日期拆分成独立文件
        String file_split = "";
        if(args != null && args.length > 0){
            master = args[0].trim();
            input = args[1].trim();
            item_input = args[2].trim();
            hdfs_out = args[3].trim();
            split_date = args[4].trim();
            file_split = args[5].trim();
        }
        
        if(item_input != null && !item_input.equals("null")){
            //初始化垂直商品列表
            BufferedReader br = new BufferedReader(new FileReader(item_input));
            String l = "";
            br.readLine();
            while((l = br.readLine()) != null){
                String[] ls = l.split(",");
                if(ls != null && !target_items.contains(ls[0])){
                    target_items.add(ls[0]);
                }
            }
            br.close();
        }
        
        //生成分割日期集
        
        if(split_date != null && !split_date.equals("null")){
            //转换为小时
            String[] ls = split_date.split("-");
            if(ls != null ){
                int month = Integer.parseInt(ls[0]);
                int day = Integer.parseInt(ls[1]);
                
                while(!(month == 12 && day == 19)){
                    long hour = 0;
                    if(month == 11){
                        hour = (day - 18) * 24;
                    }
                    else if(month == 12){
                        hour = 13 * 24 + (day - 1) * 24;
                    }
                    split_dates.put(month + "-" + day, hour);
                    month++;
                    day++;
                }
            }
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
        JavaPairRDD<String,InteractionRecord> pairs = lines.mapToPair(new PairFunction<String,String,InteractionRecord>(){

            public Tuple2<String, InteractionRecord> call(String arg0) throws Exception {
                if(arg0 != null && arg0.length() > 0){
                    String[] fields = arg0.split(",");
                    if(fields != null){
                        //以user_id为主键
                        String key = null;
                        InteractionRecord record = new InteractionRecord();
                        if(fields[0] != null && fields[0].length() > 0){
                            key = fields[0];
                            record.user_id = fields[0];
                        }
                        if(fields[1] != null && fields[1].length() > 0){
                            record.item_id = fields[1];
                        }
                        if(fields[2] != null && fields[2].length() > 0){
                            record.behavior_type = fields[2];
                        }
                        if(fields[3] != null && fields[3].length() > 0){
                            record.user_geohash = fields[3];
                        }
                        if(fields[4] != null && fields[4].length() > 0){
                            record.item_category = fields[4];
                        }
                        if(fields[5] != null && fields[5].length() > 0){
                            record.time = fields[5];
                            String[] al = fields[5].split(" ");
                            if(al != null){
                                //将时间转换为小时(自11-18零时起)
                                String date = al[0];
                                
                                if(date != null){
                                    String[] al2 = date.split("-");
                                    if(al2 != null && al2.length > 0){
                                        int month = Integer.parseInt(al2[1]);
                                        int day = Integer.parseInt(al2[2]);
                                        int h = Integer.parseInt(al[1]);
                                        if(month == 11){
                                            record.hour = (day - 18) * 24 + h;
                                        }
                                        else if(month == 12){
                                            record.hour = 13 * 24 + (day - 1) * 24 + h;
                                        }
                                    }
                                }
                            }
                        }
                        
                        return new Tuple2<String,InteractionRecord>(key,record);
                    }
                }
                return null;
            }
        });
        
        //只保留垂直商品列表
        JavaPairRDD<String,InteractionRecord> filter = pairs.filter(new Function<Tuple2<String,InteractionRecord>,Boolean>(){

            public Boolean call(Tuple2<String, InteractionRecord> arg0)
                    throws Exception {
               
                return target_items.contains(arg0._2.item_id);
            }});
        
        JavaPairRDD<String,Iterable<InteractionRecord>> userid_grouped = filter.groupByKey();
        
        //hehe, 忽然发现把records以user_id聚合之后可以直接计算各种特征啦,各种复杂的数据结构可以省略掉 : )
        //计算结果直接写入hdfs中
        userid_grouped.foreach(new VoidFunction<Tuple2<String,Iterable<InteractionRecord>>>(){

            public void call(Tuple2<String, Iterable<InteractionRecord>> arg0)
                    throws Exception {
                String user_id = arg0._1;
                //采用的数据结构:
                //Map<String,Map<Long,Map<Long,Map<Integer,Integer>>>>
                //item_id,split_dates(12-16\12-17\12-18),tongji_hour(1,3,12),action,counter
                Map<String,Map<Long,Map<Long,Map<Integer,Integer>>>> history = 
                        new HashMap<String,Map<Long,Map<Long,Map<Integer,Integer>>>>();
                for(InteractionRecord i : arg0._2){
                    
                    if(!history.containsKey(i.item_id)){
                        String[] arr = split_dates.keySet().toArray(new String[0]);
                        if(arr != null){
                            for(String s : arr){
                                Map<Long,Map<Long,Map<Integer,Integer>>> mm3 = new HashMap<Long,Map<Long,Map<Integer,Integer>>>();
                                
                                for(long j : tongji_hour){
                                    Map<Long,Map<Integer,Integer>> mm2 = new HashMap<Long,Map<Integer,Integer>>();
                                    Map<Integer,Integer> mm1 = new HashMap<Integer,Integer>();
                                    mm1.put(1, 0);
                                    mm1.put(2, 0);
                                    mm1.put(3, 0);
                                    mm1.put(4, 0);
                                    mm2.put(j, mm1);
                                    mm3.put(split_dates.get(s), mm2);
                                }
                                history.put(i.item_id, mm3);
                            }
                        }
                        
                    }
                  //将各条记录插入数据结构的所有合适位置
                    
                }
            }});
    }

}
