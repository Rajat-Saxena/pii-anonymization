import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main
{
    public static void main(String args[])
    {

        SparkSession spark = SparkSession
                .builder()
                .appName("PII Anonymization")
                .master("local")
                .getOrCreate();

        Dataset<Row> reader =  spark.read()
                .format("csv")
                .option("header", "true")
                .load(" ");
    }
}
