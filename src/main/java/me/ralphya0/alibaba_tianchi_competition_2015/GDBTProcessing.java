package me.ralphya0.alibaba_tianchi_competition_2015;

import java.io.BufferedReader;
import java.io.BufferedWriter;
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
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.GradientBoostedTrees;
import org.apache.spark.mllib.tree.configuration.BoostingStrategy;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.mllib.util.MLUtils;

import scala.Tuple2;

public class GDBTProcessing {

    public static void main(String[] args) throws Exception {
        
        //构造libsvm输入数据,离线测试以及生成提交结果三种功能
        String action = null;
        String file_path1 = null;
        String file_path2 = null;
        
        String gdbt_a1 = null;
        String gdbt_a2 = null;
        int gdbt_arg1 = 0;
        int gdbt_arg2 = 0;
        if(args != null){
            action = args[0].trim();
            file_path1 = args[1].trim();
            file_path2 = args[2].trim();
            gdbt_a1 = args[3].trim();
            gdbt_a2 = args[4].trim();
        }
        
        if(action != null && action.equals("input-gen")){
            BufferedReader br = new BufferedReader(new FileReader(file_path1));
            String l = "";
            BufferedWriter bw = new BufferedWriter(new FileWriter(file_path2));
            long buy = 0;
            while((l = br.readLine()) != null){
                String[] al = l.split(",");
                if(al != null){
                    //剔除user_id,item_id和type,构造libsvm文件
                    String tmp = al[al.length - 1];
                    for(int i = 2; i < al.length - 1; i ++){
                        tmp += " " + (i - 1) + ":" + al[i]; 
                    }
                    tmp += "\n";
                    bw.write(tmp);
                    if(Double.parseDouble(al[al.length - 1]) == 1){
                        buy ++;
                    }
                }
            }
            br.close();
            bw.close();
            System.out.println("输入文件构造完毕,已写入 " + file_path2);
            System.out.println("正样本个数为:" + buy);
            
        }
        else if(action != null && action.equals("offline-test")){
                gdbt_arg1 = Integer.parseInt(gdbt_a1);
                gdbt_arg2 = Integer.parseInt(gdbt_a2);
                SparkConf conf = new SparkConf().setAppName("GDBT-algorithm").setMaster("spark://tianchi-node1:7077");
                JavaSparkContext sc = new JavaSparkContext(conf);
                
                JavaRDD<LabeledPoint> train_data = MLUtils.loadLibSVMFile(sc.sc(), file_path1).toJavaRDD();
                JavaRDD<LabeledPoint> test_data = MLUtils.loadLibSVMFile(sc.sc(), file_path2).toJavaRDD();
                
                BoostingStrategy boostingStrategy = BoostingStrategy.defaultParams("Classification");
                boostingStrategy.setNumIterations(gdbt_arg1);
                boostingStrategy.getTreeStrategy().setNumClasses(2);
                boostingStrategy.getTreeStrategy().setMaxDepth(gdbt_arg2);
                Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
                boostingStrategy.treeStrategy().setCategoricalFeaturesInfo(categoricalFeaturesInfo);
                

                GradientBoostedTreesModel model =
                  GradientBoostedTrees.train(train_data, boostingStrategy);
                
                final Broadcast<GradientBoostedTreesModel> md = sc.broadcast(model);
                //现在的问题:测试集是什么,采样比率是啥意思,要用采样的数据训练模型而未采样的数据做验证?
                //合理的方式:用17号的数据训练模型(数据本身以及17号当天的购买标签),再将18号零点之前的数据带入模型让其预测18号当天是否会购买
                
                JavaPairRDD<Double,Double> prediction = test_data.mapToPair(new PairFunction<LabeledPoint,Double,Double>(){
                    @Override
                    public Tuple2<Double, Double> call(LabeledPoint arg0)
                            throws Exception {
                        return new Tuple2<Double,Double>(md.value().predict(arg0.features()), arg0.label());
                    }
                    
                });
                
                //计算F1,准确率和召回率
                long correct_num = prediction.filter(new Function<Tuple2<Double,Double>,Boolean>(){

                    @Override
                    public Boolean call(Tuple2<Double, Double> arg0)
                            throws Exception {
                        //正确预测到的
                        return arg0._1 == 1 && arg0._2 == 1;
                    }
                }).count();
                
                long pre_buy_num = prediction.filter(new Function<Tuple2<Double,Double>, Boolean>(){

                    @Override
                    public Boolean call(Tuple2<Double, Double> arg0)
                            throws Exception {

                        return arg0._1 == 1;
                    }
                }).count();
                
                long buy_num = test_data.filter(new Function<LabeledPoint,Boolean>(){

                    @Override
                    public Boolean call(LabeledPoint arg0) throws Exception {
                        // TODO Auto-generated method stub
                        return arg0.label() == 1;
                    }}).count();
                
                System.out.println();
                System.out.println("----------------------------------------------");
                System.out.println("真实购买个数 " + buy_num);
                System.out.println("模型推荐购买个数 " + pre_buy_num);
                System.out.println("模型正确预测个数 " + correct_num);
                if(buy_num != 0 && pre_buy_num != 0){
                    double precision = (double)correct_num / (double)pre_buy_num;
                    double recall = (double)correct_num / (double)buy_num;
                    double f1 = (2 * precision * recall) / (precision + recall);
                    
                    System.out.println("参数选择: NumIterations = " + gdbt_arg1 + ", MaxDepth = " + gdbt_arg2);
                    System.out.println("训练文件: " + file_path1);
                    System.out.println("预测文件: " + file_path2);
                    System.out.println("precision = " + precision);
                    System.out.println("recall = " + recall);
                    System.out.println("F1 = " + f1);
                    
                }
        }
        else if(action != null && action.equals("submit")){
           
        }
        
        
        
    }
    

}
