package me.ralphya0.alibaba_tianchi_competition_2015;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
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
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.GradientBoostedTrees;
import org.apache.spark.mllib.tree.configuration.BoostingStrategy;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;

import scala.Tuple2;

public class GDBTProcessing {

    public static void main(String[] args) throws Exception {
        
        //离线测试以及生成提交结果两种功能
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
            gdbt_arg1 = Integer.parseInt(gdbt_a1);
            gdbt_arg2 = Integer.parseInt(gdbt_a2);
        }
        
        if(action != null && action.equals("offline-test")){
            if(file_path1 != null && file_path2 != null){
                //读取并构造训练集
                List<LabeledPoint> list = new ArrayList<LabeledPoint>();
                BufferedReader br = new BufferedReader(new FileReader(file_path1));
                String l = "";
                while((l = br.readLine()) != null){
                    String[] al = l.split(",");
                    if(al != null){
                        //剔除user_id,item_id和type
                        double[] features = new double[al.length - 3];
                        for(int i = 2; i < al.length - 1; i ++){
                            features[i - 2] = Double.parseDouble(al[i]);
                            
                        }
                        LabeledPoint pt = new LabeledPoint(Double.parseDouble(al[al.length - 1]), Vectors.dense(features));
                        list.add(pt);
                    }
                }
                br.close();
                System.out.println("训练集构造完毕");
                
                long true_buy_num = 0;
                
                //读取并构造预测集
                List<LabeledPoint> list2 = new ArrayList<LabeledPoint>();
                BufferedReader br2 = new BufferedReader(new FileReader(file_path2));
                String l2 = "";
                while((l2 = br2.readLine()) != null){
                    String[] al = l2.split(",");
                    if(al != null){
                        double[] features = new double[al.length - 3];
                        for(int i = 2; i < al.length - 1; i ++){
                            features[i - 2] = Double.parseDouble(al[i]);
                            
                        }
                        //测试集的label仅用于验证模型预测结果的准确性
                        LabeledPoint pt = new LabeledPoint(Double.parseDouble(al[al.length - 1]), Vectors.dense(features));
                        list2.add(pt);
                        if(Double.parseDouble(al[al.length - 1]) == 1){
                            true_buy_num ++;
                        }
                    }
                }
                br2.close();
                
                System.out.println("测试集构造完成");
                

                SparkConf conf = new SparkConf().setAppName("GDBT-algorithm").setMaster("spark://tianchi-node1:7077");
                JavaSparkContext sc = new JavaSparkContext(conf);
                
                JavaRDD<LabeledPoint> train_data = sc.parallelize(list);
                JavaRDD<LabeledPoint> test_data = sc.parallelize(list2);
                
                BoostingStrategy boostingStrategy = BoostingStrategy.defaultParams("Classification");
                boostingStrategy.setNumIterations(gdbt_arg1);
                boostingStrategy.getTreeStrategy().setNumClasses(2);
                boostingStrategy.getTreeStrategy().setMaxDepth(gdbt_arg2);
                Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
                boostingStrategy.treeStrategy().setCategoricalFeaturesInfo(categoricalFeaturesInfo);
                

                final GradientBoostedTreesModel model =
                  GradientBoostedTrees.train(train_data, boostingStrategy);

                //现在的问题:测试集是什么,采样比率是啥意思,要用采样的数据训练模型而未采样的数据做验证?
                //合理的方式:用17号的数据训练模型(数据本身以及17号当天的购买标签),再将18号零点之前的数据带入模型让其预测18号当天是否会购买
                
                JavaPairRDD<Double,Double> prediction = test_data.mapToPair(new PairFunction<LabeledPoint,Double,Double>(){
                    @Override
                    public Tuple2<Double, Double> call(LabeledPoint arg0)
                            throws Exception {
                        return new Tuple2<Double,Double>(model.predict(arg0.features()), arg0.label());
                    }
                    
                });
                
                //计算F1,准确率和召回率
                long correct_num = prediction.filter(new Function<Tuple2<Double,Double>,Boolean>(){

                    @Override
                    public Boolean call(Tuple2<Double, Double> arg0)
                            throws Exception {
                        //正确预测到的
                        return arg0._1 == arg0._2;
                    }
                }).count();
                
                long pre_buy_num = prediction.filter(new Function<Tuple2<Double,Double>, Boolean>(){

                    @Override
                    public Boolean call(Tuple2<Double, Double> arg0)
                            throws Exception {

                        return arg0._1 == 1.0;
                    }
                }).count();
                
                if(true_buy_num != 0 && pre_buy_num != 0){
                    double precision = correct_num / pre_buy_num;
                    double recall = correct_num / true_buy_num;
                    double f1 = (2 * precision * recall) / (precision + recall);
                    System.out.println();
                    System.out.println("----------------------------------------------");
                    System.out.println("训练文件: " + file_path1);
                    System.out.println("预测文件: " + file_path2);
                    System.out.println("precision = " + precision);
                    System.out.println("recall = " + recall);
                    System.out.println("F1 = " + f1);
                    
                }
            }
        }
        else if(action != null && action.equals("submit")){
           
        }
        
        
        
    }
    

}
