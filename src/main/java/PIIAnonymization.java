import edu.stanford.nlp.ling.CoreAnnotation;
import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.ling.CoreLabel;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.util.CoreMap;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public class PIIAnonymization
{
    private Properties props;
    private StanfordCoreNLP pipeline;
    private Annotation annotation;

    public PIIAnonymization()
    {
        this.props = new Properties();
        this.props.put("annotators", "tokenize, ssplit, ner, regexner");
    }

    public JavaPairRDD<String, Long> anonymizeData(JavaPairRDD<String, Long> anonRdd)
    {
        anonRdd = anonRdd.mapPartitionsToPair((PairFlatMapFunction<Iterator<Tuple2<String,Long>>, String, Long>) partition ->
        {
            List<Tuple2<String, Long>> list = new ArrayList<>();

            pipeline = new StanfordCoreNLP(props);

            while (partition.hasNext())
            {
                Tuple2<String, Long> tuple = partition.next();
                Tuple2<String, Long> result;

                if (tuple._1.equals(" "))
                    result = new Tuple2<>(tuple._1, tuple._2);
                else {
                    String line = classifyText(tuple._1, pipeline);
                    result = new Tuple2<>(line, tuple._2);
                }

                list.add(result);
            }

            return list;
        });
        return anonRdd;
    }

    private String classifyText(String line, StanfordCoreNLP pipeline)
    {
        // Create an empty Annotation
        annotation = new Annotation(line);

        // Run all annotators on this text
        pipeline.annotate(annotation);

        // These are all the sentences in this document
        // A CoreMap is essentially a Map that uses class objects as keys and has values with custom types
        List<CoreMap> sentences = annotation.get(CoreAnnotations.SentencesAnnotation.class);
        for (CoreMap sentence: sentences)
        {
            // Traversing the words in the current sentence
            // A CoreLabel is a CoreMap with additional token-specific methods
            for (CoreLabel token: sentence.get(CoreAnnotations.TokensAnnotation.class))
            {

            }
        }

        return line;
    }
}
