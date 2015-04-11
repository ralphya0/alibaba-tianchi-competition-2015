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
import org.apache.spark.api.java.function.PairFlatMapFunction;
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


//记录计算得到的特征
class Features implements Serializable{
    String user_id;
    String item_id;
    
    int tongji_feature1;
    int tongji_feature2;
    int tongji_feature3;
    int tongji_feature4;
    int tongji_feature5;
    int tongji_feature6;
    int tongji_feature7;
    int tongji_feature8;
    int tongji_feature9;
    int tongji_feature10;
    int tongji_feature11;
    int tongji_feature12;
    int tongji_feature13;
    int tongji_feature14;
    int tongji_feature15;
    int tongji_feature16;
    int tongji_feature17;
    int tongji_feature18;
    int tongji_feature19;
    int tongji_feature20;
    int tongji_feature21;
    int tongji_feature22;
    int tongji_feature23;
    int tongji_feature24;
    int tongji_feature25;
    int tongji_feature26;
    int tongji_feature27;
    int tongji_feature28;
    int tongji_feature29;
    int tongji_feature30;
    int tongji_feature31;
    int tongji_feature32;
    
    double bilv_feature1;
    double bilv_feature2;
    double bilv_feature3;
    double bilv_feature4;
    double bilv_feature5;
    double bilv_feature6;
    double bilv_feature7;
    double bilv_feature8;
    double bilv_feature9;
    double bilv_feature10;
    
    double zhuanhua_feature1; 
    double zhuanhua_feature2; 
    double zhuanhua_feature3; 
    double zhuanhua_feature4; 
    double zhuanhua_feature5;
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
    final static long[] tongji_hour = {1, 3, 6, 12, 24, 72, 168, 720,-1,-2};
    
    //垂直商品列表
    final static Set<String> target_items = new HashSet<String>();
    //分割日期，如12-16 + 12-17 + 12-18(转换为小时)
    final static Map<String,Long> split_dates = new HashMap<String,Long>();
    final static List<Long> split_dates_ordered = new ArrayList<Long>();
    
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
                    split_dates_ordered.add(hour);
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
       JavaPairRDD<Long,Features> features = userid_grouped
               .flatMapToPair(new PairFlatMapFunction<Tuple2<String,Iterable<InteractionRecord>>,Long,Features>(){

                public Iterable<Tuple2<Long, Features>> call(
                        Tuple2<String, Iterable<InteractionRecord>> arg0)
                        throws Exception {
                    String user_id = arg0._1;
                    //采用的数据结构:
                    Map<String,Map<Long,Map<Long,Map<Integer,Integer>>>> history = 
                            new HashMap<String,Map<Long,Map<Long,Map<Integer,Integer>>>>();
                    
                    //注意,这是总活跃日期信息,在使用其中的信息时必须根据split_date过滤不符合要求的日期
                    Map<Integer,Map<String,Map<Integer,Integer>>> active_days = new HashMap<Integer,Map<String,Map<Integer,Integer>>>();
                    for(int i = 1118;i <= 1218;i ++){
                        if(i == 1131){
                            i = 1200;
                        }
                        else{
                            Map<String,Map<Integer,Integer>> m = new HashMap<String,Map<Integer,Integer>>();
                            active_days.put(i, m);
                        }
                    }
                    for(InteractionRecord i : arg0._2){
                        
                        if(!history.containsKey(i.item_id)){
                                Map<Long,Map<Long,Map<Integer,Integer>>> mm3 = new HashMap<Long,Map<Long,Map<Integer,Integer>>>();
                                
                                for(long s : split_dates_ordered){
                                    Map<Long,Map<Integer,Integer>> mm2 = new HashMap<Long,Map<Integer,Integer>>();
                                    for(long j : tongji_hour){
                                        Map<Integer,Integer> mm1 = new HashMap<Integer,Integer>();
                                        mm1.put(1, 0);
                                        mm1.put(2, 0);
                                        mm1.put(3, 0);
                                        mm1.put(4, 0);
                                        mm2.put(j, mm1);
                                    }
                                    
                                    mm3.put(s, mm2);
                                }
                                history.put(i.item_id, mm3);
                                
                        }
                      //将各条记录插入数据结构的所有合适位置
                      long inter_hour = i.hour;
                      int action = Integer.parseInt(i.behavior_type);
                      String brand = i.item_id;
                      String time = i.time;
                      
                      
                      //主要关注该记录发生时间与各关键时间点的距离
                      for(Long split_hour : split_dates_ordered){
                          //必须将key_hour当天及之后几天的数据排除在外!!!
                          if(inter_hour < split_hour){
                            //Map<String,Map<Long,Map<Long,Map<Integer,Integer>>>>
                              //item_id,split_dates(12-16\12-17\12-18),tongji_hour(1,3,12),action,counter
                              //long[] tongji_hour = {1, 3, 6, 12, 24, 72, 168, 720};
                              //范围重叠! 1,1~3,1~6,1~12,1~24,1~72
                              for(long tezheng_h : tongji_hour){
                                  if(split_hour - inter_hour <= tezheng_h){
                                      Map<Long,Map<Long,Map<Integer,Integer>>> m1 = history.get(brand);
                                      Map<Long,Map<Integer,Integer>> m2 = m1.get(split_hour);
                                      Map<Integer,Integer> m3 = m2.get(tezheng_h);
                                      m3.put(action, m3.get(action) + 1);
                                  }
                              }
                              //第一次访问及最后一次访问
                              //改!!!!!!!!!!!!!!!
                              if(history.get(brand).get(split_hour).containsKey(-1) && history.get(brand).get(split_hour).containsKey(-2)){
                                  if(history.get(brand).get(split_hour).get(-1).get(action) == 0
                                          && history.get(brand).get(split_hour).get(-2).get(action) == 0){
                                      history.get(brand).get(split_hour).get(-1).put(action, (int) inter_hour);
                                      history.get(brand).get(split_hour).get(-2).put(action, (int) inter_hour);
                                  }
                                  else if(history.get(brand).get(split_hour).get(-1).get(action) > inter_hour){
                                      history.get(brand).get(split_hour).get(-1).put(action, (int) inter_hour);
                                  }
                                  else if(history.get(brand).get(split_hour).get(-2).get(action) < inter_hour){
                                      history.get(brand).get(split_hour).get(-2).put(action,(int) inter_hour);
                                  }
                              }
                              
                              //更新该对商品的访问日期及总活跃日期信息
                              String[] al = time.split(" ");
                              if(al != null){
                                  String[] al2 = al[0].split("-");
                                  if(al2 != null){
                                      String date_str = "-" + al2[1] + al2[2];
                                      int key = Integer.parseInt(date_str);
                                      Map<Integer,Integer> m5 = history.get(brand).get(split_hour).get(key);
                                      m5.put(action, m5.get(action) + 1);
                                      
                                      //active_days.get(key).put(action, active_days.get(key).get(action) + 1);
                                  }
                              }
                          }
                      }
                    }
                    
                    //Map<String,Map<Long,Map<Long,Map<Integer,Integer>>>>
                    //item_id,split_dates(12-16\12-17\12-18),tongji_hour(1,3,12),action,counter
                    //records插入结束,借助history对各个brand计算各项特征
                    String[] brands = history.keySet().toArray(new String[0]);
                    if(brands != null){
                        for(String bid : brands){
                            Map<Long,Map<Long,Map<Integer,Integer>>> m1 = history.get(bid);
                            if(m1 != null){
                                Long[] split_hours = m1.keySet().toArray(new Long[0]);
                                if(split_hours != null){
                                    for(long split_h : split_hours){
                                        Map<Long,Map<Integer,Integer>> m2 = m1.get(split_h);
                                      
                                        if(m2 != null){
                                            //不同的split_h输出独立的Feature
                                            Features f = new Features();
                                            f.user_id = user_id;
                                            f.item_id = bid;
                                            
                                            //final static long[] tongji_hour = {1, 3, 6, 12, 24, 72, 168, 720,-1,-2};
                                            //因为tongji_hour中元素顺序与目标特征之间没有关联,所以只能手动逐个计算
                                            //先计算统计特征
                                            if(m2.containsKey(1)){
                                                Map<Integer,Integer> m3 = m2.get(1);
                                                int view_ct = m3.get(1);
                                                int shoucang_ct = m3.get(2);
                                                int cart_ct = m3.get(3);
                                                int buy_ct = m3.get(4);
                                                //开始计算与"之前1小时"有关的统计特征
                                                f.tongji_feature1 = view_ct;
                                                f.tongji_feature2 = shoucang_ct;
                                                f.tongji_feature3 = cart_ct;
                                                f.tongji_feature4 = buy_ct;
                                            }
                                            if(m2.containsKey(6)){
                                                Map<Integer,Integer> m3 = m2.get(6);
                                                f.tongji_feature5 = m3.get(1);
                                                f.tongji_feature6 = m3.get(2);
                                                f.tongji_feature7 = m3.get(3);
                                                f.tongji_feature8 = m3.get(4);
                                            }
                                            if(m2.containsKey(24)){
                                                Map<Integer,Integer> m3 = m2.get(24);
                                                f.tongji_feature9 = m3.get(1);
                                                f.tongji_feature10 = m3.get(2);
                                                f.tongji_feature11 = m3.get(3);
                                                f.tongji_feature12 = m3.get(4);
                                            }
                                            if(m2.containsKey(72)){
                                                Map<Integer,Integer> m3 = m2.get(72);
                                                f.tongji_feature13 = m3.get(1);
                                                f.tongji_feature14 = m3.get(2);
                                                f.tongji_feature15 = m3.get(3);
                                                f.tongji_feature16 = m3.get(4);
                                            }
                                            if(m2.containsKey(168)){
                                                Map<Integer,Integer> m3 = m2.get(168);
                                                f.tongji_feature17 = m3.get(1);
                                                f.tongji_feature18 = m3.get(2);
                                                f.tongji_feature19 = m3.get(3);
                                                f.tongji_feature20 = m3.get(4);
                                            }
                                            if(m2.containsKey(720)){
                                                Map<Integer,Integer> m3 = m2.get(720);
                                                f.tongji_feature21 = m3.get(1);
                                                f.tongji_feature22 = m3.get(2);
                                                f.tongji_feature23 = m3.get(3);
                                                f.tongji_feature24 = m3.get(4);
                                            }
                                            if(m2.containsKey(-2) && m2.containsKey(-1)){
                                                Map<Integer,Integer> m3 = m2.get(-2);
                                                f.tongji_feature25 = (int) (split_h - m3.get(1));
                                                f.tongji_feature26 = (int) (split_h - m3.get(2));
                                                f.tongji_feature27 = (int) (split_h - m3.get(3));
                                                f.tongji_feature28 = (int) (split_h - m3.get(4));
                                                
                                                Map<Integer,Integer> m4 = m2.get(-1);
                                                f.tongji_feature29 = m3.get(1) - m4.get(1);
                                                f.tongji_feature30 = m3.get(2) - m4.get(2);
                                                f.tongji_feature31 = m3.get(3) - m4.get(3);
                                                f.tongji_feature32 = m3.get(4) - m4.get(4);
                                            }
                                            
                                            //计算比率特征
                                            //当前split_hour之前用户对所有品牌的各项访问总计数
                                            int total_1_ct = 0;
                                            int total_2_ct = 0;
                                            int total_3_ct = 0;
                                            int total_4_ct = 0;
                                            int user_active_days = 0;
                                            int user_1_days = 0;
                                            int user_2_days = 0;
                                            int user_3_days = 0;
                                            int user_4_days = 0;
                                            
                                            String[] brands_tmp = history.keySet().toArray(new String[0]);
                                            for(String b_name : brands_tmp){
                                                Map<Long,Map<Integer,Integer>> m3 = history.get(b_name).get(split_h);
                                                total_1_ct += m3.get(720).get(1);
                                                total_2_ct += m3.get(720).get(2);
                                                total_3_ct += m3.get(720).get(3);
                                                total_4_ct += m3.get(720).get(4);
                                                
                                                for(int h = -1218; h <= -1118; h ++){
                                                    //user_active
                                                }
                                            }
                                            
                                            
                                        }
                                        
                                    }
                                }
                            }
                            
                            
                        }
                    }
                    return null;
                }});
    }

}


/*//-1118用来记录用户在11-18日是否对该商品产生访问行为.因为插入数据时会进行过滤,所以可以放心为每个
//split_hour分配30个存储区
for(int k = -1218;k <= -1118;k ++){
    if(k == -1200){
        k = -1131;
    }
    else{
        Map<Integer,Integer> m = new HashMap<Integer,Integer>();
        m.put(1, 0);
        m.put(2, 0);
        m.put(3, 0);
        m.put(4, 0);
        mm2.put((long) k, m);
    }
    
    //另外还需记录各品牌第一次和最后一次访问时间,设置一对特殊的tongji_hour 来表征这两项信息,-1表示第一次,-2表示最后一次
                                    Map<Integer,Integer> tmp1 = new HashMap<Integer,Integer>();
                                    tmp1.put(1, 0);
                                    tmp1.put(2, 0);
                                    tmp1.put(3, 0);
                                    tmp1.put(4, 0);
                                    
                                    mm2.put((long) -1, tmp1);
                                    
                                    Map<Integer,Integer> tmp2 = new HashMap<Integer,Integer>();
                                    tmp2.put(1, 0);
                                    tmp2.put(2, 0);
                                    tmp2.put(3, 0);
                                    tmp2.put(4, 0);
                                    
                                    mm2.put((long) -2, tmp2);
}*/