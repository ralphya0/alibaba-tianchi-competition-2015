package me.ralphya0.alibaba_tianchi_competition_2015;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.GradientBoostedTrees;
import org.apache.spark.mllib.tree.configuration.BoostingStrategy;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;

public class GDBTProcessing {

    public static void main(String[] args) throws Exception {

        String file_path1 = null;
        String file_path2 = null;
        
        if(args != null){
            file_path1 = args[0].trim();
            file_path2 = args[1].trim();
        }
        
        if(file_path1 != null){
            //读取并构造JavaRDD<LabeledPoint>
            List<LabeledPoint> list = new ArrayList<LabeledPoint>();
            BufferedReader br = new BufferedReader(new FileReader(file_path1));
            String l = "";
            while((l = br.readLine()) != null){
                String[] al = l.split(",");
                if(al != null){
                    //剔除user_id,item_id和type
                    double[] features = new double[al.length - 3];
                    for(int i = 2; i < al.length - 1; i ++){
                        features[i] = Double.parseDouble(al[i]);
                        
                    }
                    LabeledPoint pt = new LabeledPoint(Double.parseDouble(al[al.length - 1]), Vectors.dense(features));
                    list.add(pt);
                }
            }
            br.close();
            System.out.println("训练集构造完毕");

            SparkConf conf = new SparkConf().setAppName("GDBT-algorithm").setMaster("spark://tianchi-node1:7077");
            JavaSparkContext sc = new JavaSparkContext(conf);
            
            JavaRDD<LabeledPoint> train_data = sc.parallelize(list);
            BoostingStrategy boostingStrategy = BoostingStrategy.defaultParams("Classification");
            boostingStrategy.setNumIterations(10);
            boostingStrategy.getTreeStrategy().setNumClasses(2);
            boostingStrategy.getTreeStrategy().setMaxDepth(7);
            Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
            boostingStrategy.treeStrategy().setCategoricalFeaturesInfo(categoricalFeaturesInfo);
            

            final GradientBoostedTreesModel model =
              GradientBoostedTrees.train(train_data, boostingStrategy);

        }
        
    }
    

}
