package me.ralphya0.alibaba_tianchi_competition_2015;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
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
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

class InteractionRecord implements Serializable{
    String user_id;
    String item_id;
    String behavior_type;
    String user_geohash;
    String item_category;
    //自11.18日零时起的小时数.因为选取的特征精度为小时
    int hour;
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
    
    //重构代码~
    public static void main(String[] args) throws Exception {
            String input = "null";
            String output = "null";
            //训练数据分割日期(输入12-16,则分割出12-16,12-17,12-18和 #12-19# 四个特征文件)
            String split_date = "null";
            String action = "null";
            if(args != null && args.length > 0){
                action = args[0].trim();
                input = args[1].trim();
                output = args[2].trim();
                split_date = args[3].trim();
            }
            
            if(action.equals("filter")){
                //过滤原始数据
              //垂直商品
                final Set<String> target_items = new HashSet<String>();
                
                    //String item_input = "/home/tianchi/project-base/tianchi/yaoxin/data/tianchi_mobile_recommend_train_item.csv";
                    //String outputs = "/home/tianchi/project-base/tianchi/yaoxin/result/train_user_filtered.csv";
                        //初始化垂直商品列表
                        BufferedReader br = new BufferedReader(new FileReader(input));
                        String l = "";
                        //br.readLine();
                        while((l = br.readLine()) != null){
                            String[] ls = l.split(",");
                            if(ls != null && !target_items.contains(ls[0])){
                                target_items.add(ls[0]);
                            }
                        }
                        br.close();
                        System.out.println("垂直商品读取完毕: 共有垂直商品" + target_items.size());
                        
                        SparkConf conf = new SparkConf().setAppName("records-filter").setMaster("spark://tianchi-node1:7077");
                        JavaSparkContext sc = new JavaSparkContext(conf);
                        
                        //垂直商品列表
                        final Broadcast<Set<String>> target = sc.broadcast(target_items);
                        
                        JavaRDD<String> lines = sc.textFile("file:/home/tianchi/project-base/tianchi/yaoxin/data/tianchi_mobile_recommend_train_user.csv")
                                .filter(new Function<String,Boolean>(){
                            
                            public Boolean call(String arg0) throws Exception {
                                //去除文件头并且将不感兴趣的商品过滤掉
                                String[] al = arg0.split(",");
                                if(al != null){
                                    if(al[0].equals("user_id"))
                                        return false;
                                    
                                    if(target.value().contains(al[1]))
                                        return true;
                                }
                                return false;
                            }});
                        
                        System.out.println("filter()过滤完毕!剩余记录数为 " + lines.count());
                        
                        //将过滤后的结果保存起来作为后续计算的输入文件
                        List<String> records = lines.collect();
                        StringBuilder sb = new StringBuilder();
                        for(String s : records){
                            sb.append(s + "\n");
                        }
                        
                        BufferedWriter bw = new BufferedWriter(new FileWriter(output));
                        bw.write(sb.toString());
                        bw.close();
                        System.out.println("过滤结果已写入输出文件 " + output);
                
            }
            else if(action.equals("extraction")){
                //计算统计信息
                if(split_date != null && !split_date.equals("null")){
                    //转换为小时
                    String[] ls = split_date.split("-");
                    if(ls != null ){
                        //假设month只为12
                        int day = Integer.parseInt(ls[1]);
                        int hour = 0;
                        hour = 13 * 24 + (day - 1) * 24;
                        String day_str = "";
                        if(day >= 10){
                            day_str = ls[1];
                        }
                        else{
                            day_str = "0" + ls[1];
                        }
                        int split_day_int = Integer.parseInt(ls[0] + day_str);
                        System.out.println("中文--split_dates " + split_date + "已转换为hour: " + hour);
                        
                        //统计时间点(与split_hour的时间差).
                        final Integer[] tongji_ho = {1, 3, 6, 12, 24, 72, 168, 720};
                        SparkConf conf = new SparkConf().setAppName("feature-extraction").setMaster("spark://tianchi-node1:7077");
                        JavaSparkContext sc = new JavaSparkContext(conf);
                        
                        //其他需要使用的变量
                        final Broadcast<Integer[]> tongji_hour = sc.broadcast(tongji_ho);
                        final Broadcast<Integer> split_hour = sc.broadcast(hour);
                        final Broadcast<Integer> split_int = sc.broadcast(split_day_int);
                        
                        //读取过滤后的输入文件
                        JavaRDD<String> lines = sc.textFile(input);
                        
                        //将原始记录封装对象,主键为user_id
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
                                                int h = Integer.parseInt(al[1]);
                                                
                                                if(date != null){
                                                    String[] al2 = date.split("-");
                                                    if(al2 != null && al2.length > 0){
                                                        int month = Integer.parseInt(al2[1]);
                                                        int day = Integer.parseInt(al2[2]);
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
                        
                     //计算split_date的各项特征
                     JavaPairRDD<String,Iterable<InteractionRecord>> userid_grouped = pairs.groupByKey();
                        
                     JavaRDD<Features> features = userid_grouped
                             .flatMap(new FlatMapFunction<Tuple2<String,Iterable<InteractionRecord>>,Features>(){
                              public Iterable<Features> call(
                                      Tuple2<String,Iterable<InteractionRecord>> arg0)
                                      throws Exception {
                                  //结果集
                                  List<Features> res = new ArrayList<Features>();
                                  String user_id = arg0._1;
                                  //采用的数据结构:
                                  Map<String,Map<Integer,Map<Integer,Integer>>> history = 
                                          new HashMap<String,Map<Integer,Map<Integer,Integer>>>();
                                  
                                  //用active_days.size()判断当天用户是否产生访问行为
                                  //date(e.g. 1128),brand_id,action,counter
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
                                      //必须先将timestamp不符合要求的记录过滤掉!!!
                                      if(i.hour >= split_hour.value()){
                                          continue;
                                      }
                                      
                                      
                                      if(!history.containsKey(i.item_id)){
                                              Map<Integer,Map<Integer,Integer>> mm3 = new HashMap<Integer,Map<Integer,Integer>>();
                                              
                                              for(int j : tongji_hour.value()){
                                                  Map<Integer,Integer> mm1 = new HashMap<Integer,Integer>();
                                                  mm1.put(1, 0);
                                                  mm1.put(2, 0);
                                                  mm1.put(3, 0);
                                                  mm1.put(4, 0);
                                                  mm3.put(j, mm1);
                                              }
                                              //另外还需记录各品牌第一次和最后一次访问时间,设置一对特殊的tongji_hour 来表征这两项信息,-1表示第一次,-2表示最后一次
                                              Map<Integer,Integer> tmp1 = new HashMap<Integer,Integer>();
                                              tmp1.put(1, 0);
                                              tmp1.put(2, 0);
                                              tmp1.put(3, 0);
                                              tmp1.put(4, 0);
                                              
                                              mm3.put(-1, tmp1);
                                              
                                              Map<Integer,Integer> tmp2 = new HashMap<Integer,Integer>();
                                              tmp2.put(1, 0);
                                              tmp2.put(2, 0);
                                              tmp2.put(3, 0);
                                              tmp2.put(4, 0);
                                              
                                              mm3.put(-2, tmp2);
                                              
                                              history.put(i.item_id, mm3);
                                      }
                                      
                                    //将各条记录插入数据结构的所有合适位置
                                    int inter_hour = i.hour;
                                    int action = Integer.parseInt(i.behavior_type);
                                    String brand = i.item_id;
                                    String time = i.time;
                                    
                                    //更新active_days信息
                                    if(time != null && time.length() > 0){
                                        String[] al = time.split(" ");
                                        if(al != null){
                                            String[] al2 = al[0].split("-");
                                            if(al2 != null){
                                                //生成日期
                                                int key = Integer.parseInt(al2[1] + al2[2]);
                                                Map<String,Map<Integer,Integer>> m4 = active_days.get(key);
                                                if(!m4.containsKey(brand)){
                                                    Map<Integer,Integer> m5 = new HashMap<Integer,Integer>();
                                                    m5.put(1, 0);
                                                    m5.put(2, 0);
                                                    m5.put(3, 0);
                                                    m5.put(4, 0);
                                                    m4.put(brand, m5);
                                                }
                                                m4.get(brand).put(action, m4.get(brand).get(action) + 1);
                                            }
                                       }
                                    }
                                    
                                    //主要关注该记录发生时间与关键时间点的距离
                                    //必须将split_hour当天及之后几天的数据排除在外!!!
                                     if(inter_hour < split_hour.value()){
                                       //Map<String,Map<Long,Map<Long,Map<Integer,Integer>>>>
                                         //item_id,split_dates(12-16\12-17\12-18),tongji_hour(1,3,12),action,counter
                                         //long[] tongji_hour = {1, 3, 6, 12, 24, 72, 168, 720};
                                         //范围重叠! 1,1~3,1~6,1~12,1~24,1~72
                                         for(int tezheng_h : tongji_hour.value()){
                                             if(split_hour.value() - inter_hour <= tezheng_h){
                                                 Map<Integer,Map<Integer,Integer>> m1 = history.get(brand);
                                                 Map<Integer,Integer> m3 = m1.get(tezheng_h);
                                                 m3.put(action, m3.get(action) + 1);
                                             }
                                         }
                                         //第一次访问及最后一次访问
                                         if(history.get(brand).containsKey(-1) && history.get(brand).containsKey(-2)){
                                             if(history.get(brand).get(-1).get(action) == 0
                                                     && history.get(brand).get(-2).get(action) == 0){
                                                 history.get(brand).get(-1).put(action, inter_hour);
                                                 history.get(brand).get(-2).put(action, inter_hour);
                                             }
                                             if(history.get(brand).get(-1).get(action) > inter_hour){
                                                history.get(brand).get(-1).put(action, inter_hour);
                                             }
                                             if(history.get(brand).get(-2).get(action) < inter_hour){
                                                 history.get(brand).get(-2).put(action, inter_hour);
                                             }
                                         }
                                     }
                                  }
                                  
                                  
                                  //records已经插入到数据结构中,借助history和active_days对各个brand计算各项特征
                                  String[] brands = history.keySet().toArray(new String[0]);
                                  if(brands != null){
                                      for(String bid : brands){
                                          Map<Integer,Map<Integer,Integer>> m1 = history.get(bid);
                                          if(m1 != null){
                                                  //对每个brand,生成统计数据
                                                  Features f = new Features();
                                                  f.user_id = user_id;
                                                  f.item_id = bid;
                                                  
                                                  //final static long[] tongji_hour = {1, 3, 6, 12, 24, 72, 168, 720};
                                                  //因为tongji_hour中元素顺序与目标特征之间没有关联,所以只能手动逐个计算
                                                  //先计算统计特征
                                                  if(m1.containsKey(1)){
                                                      Map<Integer,Integer> m3 = m1.get(1);
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
                                                  if(m1.containsKey(6)){
                                                      Map<Integer,Integer> m3 = m1.get(6);
                                                      f.tongji_feature5 = m3.get(1);
                                                      f.tongji_feature6 = m3.get(2);
                                                      f.tongji_feature7 = m3.get(3);
                                                      f.tongji_feature8 = m3.get(4);
                                                  }
                                                  if(m1.containsKey(24)){
                                                      Map<Integer,Integer> m3 = m1.get(24);
                                                      f.tongji_feature9 = m3.get(1);
                                                      f.tongji_feature10 = m3.get(2);
                                                      f.tongji_feature11 = m3.get(3);
                                                      f.tongji_feature12 = m3.get(4);
                                                  }
                                                  if(m1.containsKey(72)){
                                                      Map<Integer,Integer> m3 = m1.get(72);
                                                      f.tongji_feature13 = m3.get(1);
                                                      f.tongji_feature14 = m3.get(2);
                                                      f.tongji_feature15 = m3.get(3);
                                                      f.tongji_feature16 = m3.get(4);
                                                  }
                                                  if(m1.containsKey(168)){
                                                      Map<Integer,Integer> m3 = m1.get(168);
                                                      f.tongji_feature17 = m3.get(1);
                                                      f.tongji_feature18 = m3.get(2);
                                                      f.tongji_feature19 = m3.get(3);
                                                      f.tongji_feature20 = m3.get(4);
                                                  }
                                                  if(m1.containsKey(720)){
                                                      Map<Integer,Integer> m3 = m1.get(720);
                                                      f.tongji_feature21 = m3.get(1);
                                                      f.tongji_feature22 = m3.get(2);
                                                      f.tongji_feature23 = m3.get(3);
                                                      f.tongji_feature24 = m3.get(4);
                                                  }
                                                  if(m1.containsKey(-2) && m1.containsKey(-1)){
                                                      Map<Integer,Integer> m3 = m1.get(-2);
                                                      f.tongji_feature25 = split_hour.value() - m3.get(1);
                                                      f.tongji_feature26 = split_hour.value() - m3.get(2);
                                                      f.tongji_feature27 = split_hour.value() - m3.get(3);
                                                      f.tongji_feature28 = split_hour.value() - m3.get(4);
                                                      
                                                      Map<Integer,Integer> m4 = m1.get(-1);
                                                      f.tongji_feature29 = m3.get(1) - m4.get(1);
                                                      f.tongji_feature30 = m3.get(2) - m4.get(2);
                                                      f.tongji_feature31 = m3.get(3) - m4.get(3);
                                                      f.tongji_feature32 = m3.get(4) - m4.get(4);
                                                  }
                                                  
                                                  //计算比率特征
                                                  //该split_hour之前用户对所有品牌的各项访问总计数
                                                  int total_1_ct = 0;
                                                  int total_2_ct = 0;
                                                  int total_3_ct = 0;
                                                  int total_4_ct = 0;
                                                  int user_active_days = 0;
                                                  int user_1_days = 0;
                                                  int user_2_days = 0;
                                                  int user_3_days = 0;
                                                  int user_4_days = 0;
                                                  int brand_1234_times = 0;
                                                  int x_day_total_1234_times = 0;
                                                  int brand_1234_days = 0;
                                                  int brand_4_days = 0;
                                                  
                                                  String[] brands_tmp = history.keySet().toArray(new String[0]);
                                                  for(String b_name : brands_tmp){
                                                      Map<Integer,Map<Integer,Integer>> m3 = history.get(b_name);
                                                      total_1_ct += m3.get(720).get(1);
                                                      total_2_ct += m3.get(720).get(2);
                                                      total_3_ct += m3.get(720).get(3);
                                                      total_4_ct += m3.get(720).get(4);
                                                      
                                                  }
                                                  Integer[] days = active_days.keySet().toArray(new Integer[0]);
                                                  if(days != null){
                                                     for(int t : days){
                                                         //必须注意split_hour的限制!!!
                                                         if(t < split_int.value() && active_days.get(t).size() > 0){
                                                             user_active_days ++;
                                                             Map<String,Map<Integer,Integer>> m5 = active_days.get(t);
                                                             //更新品牌访问次数和品牌访问天数
                                                             if(m5.containsKey(bid)){
                                                                 brand_1234_days ++;
                                                                 Map<Integer,Integer> m6 = m5.get(bid);
                                                                 brand_1234_times += m6.get(1);
                                                                 brand_1234_times += m6.get(2);
                                                                 brand_1234_times += m6.get(3);
                                                                 brand_1234_times += m6.get(4);
                                                                 
                                                                 if(m6.get(4) > 0){
                                                                     brand_4_days ++;
                                                                 }
                                                             }
                                                             String[] bs = m5.keySet().toArray(new String[0]);
                                                             if(bs != null){
                                                                 
                                                                 //更新浏览天数、收藏天数...
                                                                 boolean flag1 = false,flag2 = false,flag3 = false,flag4 = false,flag5 = false;
                                                                 
                                                                 flag5 = m5.containsKey(bid);
                                                                 
                                                                 for(String bname : bs){
                                                                     if(!flag1 && m5.get(bname).get(1) > 0){
                                                                         user_1_days ++;
                                                                         flag1 = true;
                                                                     }
                                                                     if(!flag2 && m5.get(bname).get(2) > 0){
                                                                         user_2_days ++;
                                                                         flag2 = true;
                                                                     }
                                                                     if(!flag3 && m5.get(bname).get(3) > 0){
                                                                         user_3_days ++;
                                                                         flag3 = true;
                                                                     }
                                                                     if(!flag4 && m5.get(bname).get(4) > 0){
                                                                         user_4_days ++;
                                                                         flag4 = true;
                                                                     }
                                                                     
                                                                     if(flag5){
                                                                         x_day_total_1234_times += m5.get(bname).get(1);
                                                                         x_day_total_1234_times += m5.get(bname).get(2);
                                                                         x_day_total_1234_times += m5.get(bname).get(3);
                                                                         x_day_total_1234_times += m5.get(bname).get(4);
                                                                     }
                                                                     if(flag1 && flag2 && flag3 && flag4 && !flag5){
                                                                         break;
                                                                     }
                                                                 }
                                                             }
                                                         }
                                                     }
                                                  }
                                                  
                                                  //开始生成比率特征
                                                  if(total_1_ct > 0){
                                                      f.bilv_feature1 = m1.get(720).get(1) / total_1_ct;
                                                      f.bilv_feature2 = (m1.get(720).get(2) + m1.get(720).get(3) + m1.get(720).get(4)) / total_1_ct;
                                                  }
                                                  
                                                  if(total_4_ct > 0){
                                                      f.bilv_feature3 = m1.get(720).get(4) / total_4_ct;
                                                  }
                                                  
                                                  if(x_day_total_1234_times > 0){
                                                      f.bilv_feature4 = brand_1234_times / x_day_total_1234_times;
                                                  }
                                                  
                                                  if(total_1_ct > 0){
                                                      f.bilv_feature5 = m1.get(24).get(1) / total_1_ct;
                                                      f.bilv_feature6 = m1.get(12).get(1) / total_1_ct;
                                                      f.bilv_feature7 = m1.get(3).get(1) / total_1_ct;
                                                      f.bilv_feature8 = m1.get(1).get(1) / total_1_ct;
                                                  }
                                                  
                                                  if(user_active_days > 0){
                                                      f.bilv_feature9 = brand_1234_days / user_active_days;
                                                  }
                                                  
                                                  if(user_4_days > 0){
                                                      f.bilv_feature10 = brand_4_days / user_4_days;
                                                  }
                                                  
                                                  //计算转化特征
                                                  if(m1.get(720).get(1) > 0){
                                                      f.zhuanhua_feature1 = m1.get(720).get(4) / m1.get(720).get(1);
                                                      
                                                      f.zhuanhua_feature4 = m1.get(720).get(2) / m1.get(720).get(1);
                                                      
                                                      f.zhuanhua_feature5 = m1.get(720).get(3) / m1.get(720).get(1);
                                                  }
                                                  if(m1.get(720).get(2) > 0){
                                                      f.zhuanhua_feature2 = m1.get(720).get(4) / m1.get(720).get(2);
                                                      
                                                  }
                                                  if(m1.get(720).get(3) > 0){
                                                      f.zhuanhua_feature3 = m1.get(720).get(4) / m1.get(720).get(3);                                                  }
                                                  
                                                  res.add(f);
                                             }      
                                      }
                                  }
                                  return res;
                              }

                            });
                     
                     //输出统计结果
                     List<Features> results = features.collect();
                     
                     BufferedWriter bw = new BufferedWriter(new FileWriter(output,true));
                     bw.write("user_id,item_id,用户在前1小时浏览品牌次数,用户在前1小时收藏品牌次数,用户在前1小时加入购物车次数,用户在前1小时购买品牌次数,"
                             + "用户在前6小时浏览品牌次数,用户在前6小时收藏品牌次数,用户在前6小时加入购物车次数,用户在前6小时购买品牌次数,"
                             + "用户在前24小时浏览品牌次数,用户在前24小时收藏品牌次数,用户在前24小时加入购物车次数,用户在前24小时购买品牌次数,"
                             + "用户在前72小时浏览品牌次数,用户在前72小时收藏品牌次数,用户在前72小时加入购物车次数,用户在前72小时购买品牌次数,"
                             + "用户在前7天浏览品牌次数,用户在前7天收藏品牌次数,用户在前7天加入购物车次数,用户在前7天购买品牌次数,"
                             + "用户在前30天浏览品牌次数,用户在前30天收藏品牌次数,用户在前30天加入购物车次数,用户在前30天购买品牌次数,"
                             + "最后一次对品牌的浏览到最后一刻的时间间隔,最后一次对品牌的收藏到最后一刻的时间间隔,最后一次对品牌的加入购物车到最后一刻的时间间隔,"
                             + "最后一次对品牌的购买到最后一刻的时间间隔,用户对品牌第一次浏览与最后一次的时间间隔,用户对品牌第一次收藏与最后一次的时间间隔,"
                             + "用户对品牌第一次加入购物车与最后一次的时间间隔,用户对品牌第一次购买与最后一次的时间间隔,浏览品牌的次数/总浏览次数,"
                             + "购买收藏加入购物车总次数/总浏览的次数,购买品牌次数/总购买次数,访问品牌的那些日期中访问该品牌次数/总访问次数,"
                             + "截止前24小时浏览品牌次数/总浏览次数,截止前12小时浏览品牌次数/总浏览次数,截止前3小时浏览品牌次数/总浏览次数,"
                             + "截止前1小时浏览品牌次数/总浏览次数,用户访问品牌的天数/活跃总天数,用户购买品牌的天数/有购买行为总天数,"
                             + "用户对品牌浏览-购买转化率,用户对品牌收藏-购买转化率,用户对品牌加入购物车-购买转化率,用户对品牌浏览-收藏转化率,"
                             + "用户对品牌浏览-加入购物车转化率" + "\n");
                     for(Features f : results){
                         bw.write(f.user_id + "," + f.item_id + "," + f.tongji_feature1 + "," + f.tongji_feature2 + "," + f.tongji_feature3 + "," + f.tongji_feature4 + "," +
                                   f.tongji_feature5 + "," + f.tongji_feature6 + "," + f.tongji_feature7 + "," + f.tongji_feature8 + "," +
                                   f.tongji_feature9 + "," + f.tongji_feature10 + "," + f.tongji_feature11 + "," + f.tongji_feature12 + "," +
                                   f.tongji_feature13 + "," + f.tongji_feature14 + "," + f.tongji_feature15 + "," + f.tongji_feature16 + "," +
                                   f.tongji_feature17 + "," + f.tongji_feature18 + "," + f.tongji_feature19 + "," + f.tongji_feature20 + "," +
                                   f.tongji_feature21 + "," + f.tongji_feature22 + "," + f.tongji_feature23 + "," + f.tongji_feature24 + "," +
                                   f.tongji_feature25 + "," + f.tongji_feature26 + "," + f.tongji_feature27 + "," + f.tongji_feature28 + "," +
                                   f.tongji_feature29 + "," + f.tongji_feature30 + "," + f.tongji_feature31 + "," + f.tongji_feature32 + "," +
                                   f.bilv_feature1 + "," + f.bilv_feature2 + "," + f.bilv_feature3 + "," + f.bilv_feature4 + "," + 
                                   f.bilv_feature5 + "," + f.bilv_feature6 + "," + f.bilv_feature7 + "," + f.bilv_feature8 + "," + 
                                   f.bilv_feature9 + "," + f.bilv_feature10 + "," + 
                                   f.zhuanhua_feature1 + "," + f.zhuanhua_feature2 + "," + f.zhuanhua_feature3 + "," + 
                                   f.zhuanhua_feature4 + "," + f.zhuanhua_feature5 + "\n");
                           
                     }
                     
                     bw.close();
                     System.out.println("done! 统计结果已输出至 " + output);
                    }
                }
            }
            
            //每条记录的主键是user_id和item_id!!!
            //维护若干个数据结构来指示已经处理过的user_id+item_id的组合以避免重复计算,同时要为特征计算提供便利
            //各项特征都需要完整扫描训练数据
            //扫描过程中将同一用户的信息归并到一起然后输出到文件中(方便特征统计!!!),同时将原始数据按照日期拆分成独立文件(作为独立小功能)
            //计算过程中涉及时间的操作一律统一以小时为计量单位
    }
}