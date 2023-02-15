package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class CountPCollectionExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> p1 = pipeline.apply(TextIO.read().from("src/main/java/resources/File1.txt"));
        PCollection<Long> countPCollection = p1.apply(Count.globally());
        countPCollection.apply(ParDo.of(new DoFn<Long, Void>() {
            @ProcessElement
            public void processElement(ProcessContext context) {
                System.out.println(context.element());
            }
        }));
        pipeline.run();
    }
}
