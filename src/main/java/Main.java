import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.*;
import scala.Tuple2;

public class Main
{
    private static Dataset<Row> dataDS;
    private static Dataset<Row> tempDS;
    private static Dataset<Row> anonymizedDS;
    private static Dataset<Tuple2<String, Long>> resultDS;
    private static Logger logger = Logger.getLogger(Main.class);
    private static PIIAnonymization anonymization;

    public static void main(String args[])
    {
        if (args.length < 3)
        {
            logger.error("Incorrect number of arguments.");
        }
        else
        {
            anonymization = new PIIAnonymization();
            String sourceFilePath = args[0];
            String targetDirectory = args[1];
            String targetFilePath = targetDirectory + "/result.csv";

            SparkSession spark = SparkSession
                    .builder()
                    .appName("PII Anonymization")
                    .master("local")
                    .getOrCreate();

            logger.info("Reading source file from: " + sourceFilePath);
            dataDS = spark.read()
                    .format("csv")
                    .option("header", "true")
                    .load(sourceFilePath);

            String deleteCol = "deleteCol";
            String rowIndex = "rowIndex";
            String columnName = args[2];
            int numPartitions = 2; // Change as per Spark configuration.

            dataDS = dataDS.withColumn(rowIndex, functions.monotonicallyIncreasingId());
            tempDS = dataDS.select(columnName, rowIndex);
            dataDS = dataDS.withColumnRenamed(columnName, deleteCol);

            JavaPairRDD<String, Long> anonRdd = tempDS.javaRDD()
                    .mapToPair((PairFunction<Row, String, Long>) row ->
                    {
                        if (row.isNullAt(0))
                            return new Tuple2<>(" ", row.getLong(1));
                        else
                            return new Tuple2<>(row.getString(0), row.getLong(1));
                    })
                    .repartition(numPartitions);

            logger.info("Starting anonymization.");
            Long startTime = System.currentTimeMillis();
            JavaPairRDD<String, Long> resultRdd = PIIAnonymization.anonymizeData(anonRdd);

            resultDS = spark.createDataset(resultRdd.collect(), Encoders.tuple(Encoders.STRING(), Encoders.LONG()));

            anonymizedDS = dataDS.join(resultDS, rowIndex).drop(deleteCol).drop(rowIndex);

            logger.info("Anonymization complete. Took " + (System.currentTimeMillis() - startTime) + " (ms).");
            logger.info("Writing data to target file: " + targetFilePath);

            anonymizedDS.write().csv(targetFilePath);
        }
    }
}
