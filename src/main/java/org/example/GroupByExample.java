package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.GroupByKey;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.regex.Pattern;

/**
 * Output sum of amount of each user.
 *
 * Mona|1|1|100
 * Nora|2|2|200
 * Mona|3|3|600
 * Diya|4|4|200
 *
 * Diya   200
 * Nora   200
 * Mona   700
 *
 */
public class GroupByExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> readPCollection = pipeline.apply(TextIO.read().from("src/main/java/resources/input/groupby.txt"));
        PCollection<KV<String,String>> inputKVPCollection = readPCollection.apply(MapElements.via(new SimpleFunction<String, KV<String, String>>() {
            @Override
            public KV<String, String> apply(String input) {
                String[] array =input.split(Pattern.quote("|"));
                KV<String,String> kvMap= KV.of(array[0],array[3]);
                return kvMap;
            }
        }));
        PCollection<KV<String, Iterable<String>>> iterablePCollection = inputKVPCollection.apply(GroupByKey.<String,String>create());
        iterablePCollection.apply(MapElements.into(TypeDescriptors.voids()).via
                ((SerializableFunction<KV<String, Iterable<String>>, Void>) new SimpleFunction<KV<String, Iterable<String>>, Void>() {
            public Void apply(KV<String, Iterable<String>> inputKV) {
                Integer sum = 0;
                for (String input: inputKV.getValue()) {
                    sum = sum+ Integer.parseInt(input);
                }

                System.out.println(inputKV.getKey()+"   "+ sum);
                return null;
            }
        }));
        pipeline.run();
    }
}
