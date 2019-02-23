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

    public static void main(String args[])
    {
        SparkSession spark = SparkSession
                .builder()
                .appName("PII Anonymization")
                .master("local")
                .getOrCreate();

        dataDS = spark.read()
                .format("csv")
                .option("header", "true")
                .load(" ");
        // TODO: Update the above

        String deleteCol = "deleteCol";
        String rowIndex = "rowIndex";
        String columnName = "desc"; // TODO: Parameterize this
        int numPartitions = 2; // TODO: Shouldn't be hardcoded

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

        PIIAnonymization anonymization = new PIIAnonymization();
        JavaPairRDD<String, Long> resultRdd = anonymization.anonymizeData(anonRdd);

        resultDS = spark.createDataset(resultRdd.collect(), Encoders.tuple(Encoders.STRING(), Encoders.LONG()));

        anonymizedDS = dataDS.join(resultDS, rowIndex).drop(deleteCol).drop(rowIndex);

        anonymizedDS.write(); // TODO: Update this
    }
}
