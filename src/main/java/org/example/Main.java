package org.example;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

//import static  spark.Spark;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import scala.Tuple2;
import scala.text.Document$;

import static org.apache.spark.sql.catalyst.parser.SqlBaseLexer.*;

public class Main {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("MongoSparkConnectorIntro")
                .config("spark.mongodb.input.uri", "mongodb://localhost:27017/NotesStore.notes")
                //.config("spark.mongodb.output.uri", "mongodb://localhost:27017/UserDB.user")
                .getOrCreate();


// Create a JavaSparkContext using the SparkSession's SparkContext object
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        Dataset<Row> df = MongoSpark.load(jsc).toDF();
        //df.printSchema();

        df.createOrReplaceTempView("root");
 //       Dataset<Row> selectdf = spark.sql("SELECT * FROM root WHERE topic IN ('DSA')");
 //       selectdf.show();

//        Dataset<Row> df = MongoSpark.load(jsc).toDF();
//        df.createOrReplaceTempView("root");
//        df1=spark.sql("SELECT topic, description CASE
//                        WHEN _id=115 THEN 'hi'
//                        WHEN _id = 112 THEN 'hey' ELSE 'else' END AS topic FROM root");
//        df1.show();
        Dataset<Row> df1 = spark.read().format("com.mongodb.spark.sql.DefaultSource").load();
        Dataset<Row> df2 = spark.read().format("com.mongodb.spark.sql.DefaultSource").load();
        Dataset<Row> df3 = df1.join(df2, df1.col("_id").equalTo(df2.col("_id")));
        df3.show();

//        ===============================================================================================

        Dataset<Row> collection1 = spark.read().format("com.mongodb.spark.sql.DefaultSource")
                        .option("uri", "mongodb://localhost:27017/test.employees")
                                .load();

//        Dataset<Row> collection2 = spark.read().format("com.mongodb.spark.sql.DefaultSource")
//                        .option("uri", "mongodb://localhost:27017/test.employees")
//                                .load();
//
//        Dataset<Row> resultLeft = collection1.join(collection2, "left");
//        resultLeft.show();

        List<Person> people = Arrays.asList(new Person("Horvath", 30),
                new Person("Brad", 25),
                new Person("Jane", 35)
                ,new Person("Jay", 35)     );

// Convert the list to a Dataset
        Dataset<Person> peopleDataset = spark.createDataset(people, Encoders.bean(Person.class));

// Show the Dataset
        peopleDataset.show();

        Dataset<Row> result=collection1.join(peopleDataset, collection1.col("first_name").equalTo(peopleDataset.col("name")));
       // result.show();

        Dataset<Row> resultL=collection1.join(peopleDataset, collection1.col("first_name").equalTo(peopleDataset.col("name")), "left");
       // resultL.show();

        Dataset<Row> resultA=peopleDataset.join(collection1, peopleDataset.col("name").equalTo(collection1.col("first_name")), "leftanti");
       // resultA.show();

        Dataset<Row> resultO=peopleDataset.join(collection1, peopleDataset.col("name").equalTo(collection1.col("first_name")), "outer");
        //resultO.show();

        Dataset<Row> resultF=peopleDataset.join(collection1, peopleDataset.col("name").equalTo(collection1.col("first_name")), "full");
        resultF.show();



        jsc.close();

    }
    }
