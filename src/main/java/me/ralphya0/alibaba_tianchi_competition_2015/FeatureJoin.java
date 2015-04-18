package me.ralphya0.alibaba_tianchi_competition_2015;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;

import scala.Tuple2;

//将提取到的用户、商品、交互特征按照user_id和item_id合并起来
public class FeatureJoin {

    public static void main(String[] args) throws Exception {
        //三个待合并特征文件所在目录
        String u_input = "";
        String i_input = "";
        String ui_input = "";
        String out = "";
        //日期跨度,e.g. 12-15
        String split_day = "";
        if(args != null && args.length > 0){
            u_input = args[0].trim();
            i_input = args[1].trim();
            ui_input = args[2].trim();
            out = args[3].trim();
            split_day = args[4].trim();
        }
        
        SparkConf conf = new SparkConf().setAppName("feature-join").setMaster("spark://tianchi-node1:7077");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //读取当前日期的真实购买情况
        Map<String,List<String>> buy_history = new HashMap<String,List<String>>();
        
        String in = "/home/tianchi/project-base/tianchi/yaoxin/data/train_user_filtered.csv";
        BufferedReader br = new BufferedReader(new FileReader(in));
        String l = "";
        while((l = br.readLine()) != null){
            String[] al = l.split(",");
            if(al != null){
                //快速过滤
                if(al[2] != null && al[2].equals("4") && al[5] != null){
                    String[] al2 = al[5].split(" ");
                    if(al2 != null){
                        String[] al3 = al2[0].split("-");
                        if(al3 != null){
                            String date = al3[1] + "-" + al3[2];
                            if(date.equals(split_day)){
                                String user_id = al[0];
                                String item_id = al[1];
                                if(!buy_history.containsKey(user_id)){
                                    List<String> ls = new ArrayList<String>();
                                    buy_history.put(user_id, ls);
                                }
                                buy_history.get(user_id).add(item_id);
                            }
                        }
                    }
                }
            }
        }
        br.close();
        
        final Broadcast<Map<String,List<String>>> buy = sc.broadcast(buy_history);
        final Broadcast<String> split = sc.broadcast(split_day);
        
        //合并三个特征文件
        JavaRDD<String> ui_lines = sc.textFile(ui_input);
        
        //先把ui特征文件的第一行字段说明过滤掉然后生成join主键
        JavaPairRDD<String,String> ui_records = ui_lines.filter(new Function<String,Boolean>(){

            public Boolean call(String arg0) throws Exception {
                String[] ls = arg0.split(",");
                if(ls != null){
                    return !ls[0].equals("user_id");
                }
                return false;
            }
            
        }).mapToPair(new PairFunction<String,String,String>(){

            public Tuple2<String, String> call(String arg0)
                    throws Exception {
                String[] al = arg0.split(",");
                if(al != null){
                    String user_id = al[0];
                    return new Tuple2<String,String>(user_id,arg0);
                }
                return null;
            }});
        
        JavaPairRDD<String,String> u_records = sc.textFile(u_input)
                .mapToPair(new PairFunction<String,String,String>(){

                    public Tuple2<String, String> call(String arg0)
                            throws Exception {
                        String[] al = arg0.split(",");
                        if(al != null){
                            //记录主键
                            String user_id = al[0];
                            //需要把u feature中的user_id字段去除(防止重复出现)
                            int uid_range = arg0.indexOf(',');
                            String tmp = arg0.substring(uid_range + 1);
                            
                            return new Tuple2<String,String>(user_id,tmp);
                        }
                        return null;
                    }});
        
        //首先把ui feature和u feature合并起来,然后将该合并结果与i feature合并
        JavaPairRDD<String,String> ui_u_joined = ui_records.join(u_records)
                .mapToPair(new PairFunction<Tuple2<String,Tuple2<String,String>>,String,String>(){

                    public Tuple2<String, String> call(
                            Tuple2<String, Tuple2<String, String>> arg0)
                            throws Exception {
                        Tuple2<String,String> str = arg0._2;
                        String ui_str = str._1;
                        String u_str = str._2;
                        String[] al = ui_str.split(",");
                        if(al != null){
                            return new Tuple2<String,String>(al[1],ui_str + "," + u_str);
                        }
                        return null;
                    }});
        
        JavaPairRDD<String,String> i_records = sc.textFile(i_input)
                .mapToPair(new PairFunction<String,String,String>(){

                    public Tuple2<String, String> call(String arg0)
                            throws Exception {
                        String[] al = arg0.split(",");
                        if(al != null){
                            int index = arg0.indexOf(',');
                            String tmp = arg0.substring(index + 1);
                            return new Tuple2<String,String>(al[0],tmp);
                        }
                        return null;
                    }});
        
        JavaRDD<String> results = ui_u_joined.join(i_records)
                    .map(new Function<Tuple2<String,Tuple2<String,String>>,String>(){

                        public String call(
                                Tuple2<String, Tuple2<String, String>> arg0)
                                throws Exception {
                            //拼接字符串
                            String ui_u_str = arg0._2._1;
                            String i_str = arg0._2._2;
                            //合并12-19文件时不应包含type字段!!!
                            if(split.value().equals("12-19")){
                                return ui_u_str + "," + i_str;
                            }
                            else{
                                String[] al = ui_u_str.split(",");
                                if(al != null){
                                    String user_id = al[0];
                                    String item_id = al[1];
                                    if(buy.value().containsKey(user_id)){
                                        if(buy.value().get(user_id).contains(item_id)){
                                            return ui_u_str + "," + i_str + ",1";
                                        }
                                        else
                                            return ui_u_str + "," + i_str + ",0"; 
                                    }
                                    else{
                                        return ui_u_str + "," + i_str + ",0";
                                    }
                                }
                            }
                            return null;
                        }
                        
                    });
        //输出结果
        List<String> rs = results.collect();
        BufferedWriter bw = new BufferedWriter(new FileWriter(out));
        for(String s : rs){
            bw.write(s);
        }
        bw.close();
        System.out.println("处理结束,结果写入" + out);
    }
    
}
