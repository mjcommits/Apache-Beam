package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.example.Options.MyOptions;
import org.example.common.Common;

import java.util.regex.Pattern;

/**
 * Create a new PCollection by adding the customer name also to the first PCollection.
 * Only take values which are present in both the tables.
 *
 * Input 1:
 * 1|o1|p1|200
 * 2|o2|p2|300
 * 3|03|p1|100
 * 4|03|p1|100
 *
 *
 * Input 2:
 * 1|Mona
 * 2|Nora
 * 3|Diya
 *
 * Output:
 * 1|o1|p1|200|Mona
 * 2|o2|p2|300|Nora
 * 3|03|p1|100|Diya
 * 4|03|p1|100|null
 *
 *
 *
 */
public class LeftOuterJoinExample {
    public static void main(String[] args) {
        MyOptions options = Common.getPipelineOptions(args);
        Pipeline pipeline = Pipeline.create();
        PCollection<String> orders = pipeline.apply(TextIO.read().from(options.getInputFilePath() +"innerjoin1.txt"));
        PCollection<String> users = pipeline.apply(TextIO.read().from(options.getInputFilePath() +"innerjoin2.txt"));

        PCollection<KV<String, String>> inputKVPCollection1 = orders.apply(MapElements.via(new SimpleFunction<>() {
            @Override
            public KV<String, String> apply(String input) {
                String[] array = input.split(Pattern.quote("|"));
                KV<String, String> kvMap = KV.of(array[0], array[1] + "|" + array[2] + "|" + array[3]);
                return kvMap;
            }
        }));

        PCollection<KV<String, String>> inputKVPCollection2 = users.apply(MapElements.via(new SimpleFunction<>() {
            @Override
            public KV<String, String> apply(String input) {
                String[] array = input.split(Pattern.quote("|"));
                KV<String, String> kvMap = KV.of(array[0], array[1]);
                return kvMap;
            }
        }));

        TupleTag<String> orderTuple = new TupleTag<>();
        TupleTag<String> userTuple = new TupleTag<>();
        PCollection<KV<String, CoGbkResult>> resultGroupBy = KeyedPCollectionTuple.of(orderTuple, inputKVPCollection1).
                                                                             and(userTuple, inputKVPCollection2).
                                                                            apply(CoGroupByKey.create());
        PCollection<String> result = resultGroupBy.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {

                    @ProcessElement
                    public void processElement(ProcessContext context){
                        String key = context.element().getKey();
                        CoGbkResult coGroupByKey = context.element().getValue();
                        Iterable<String> orderDetails = coGroupByKey.getAll(orderTuple);
                        Iterable<String> userDetails = coGroupByKey.getAll(userTuple);
                        for (String order : orderDetails) {
                            if (userDetails.iterator().hasNext()) {
                                for (String user : userDetails) {
                                    context.output(key + "|" + order + "|" + user);
                                }
                            } else {
                                context.output(key + "|" + order + "|" + null);
                            }
                        }
                    }
                }));
        result.apply(TextIO.write().to(options.getOutputFilePath()+"innerJoinResult").withNumShards(1).withSuffix(".txt"));
        pipeline.run();
    }
}
