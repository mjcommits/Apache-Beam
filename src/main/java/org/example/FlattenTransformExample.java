package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;

public class FlattenTransformExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> p1 = pipeline.apply(TextIO.read().from("src/main/java/resources/File1.txt"));
        PCollection<String> p2 = pipeline.apply(TextIO.read().from("src/main/java/resources/File2.txt"));
        PCollection<String> p3 = pipeline.apply(TextIO.read().from("src/main/java/resources/File3.txt"));
        PCollectionList<String> pCollectionList = PCollectionList.of(p1).and(p2).and(p3);
        PCollection<String> finalCombinedOutput = pCollectionList.apply(Flatten.pCollections());
        finalCombinedOutput.apply(TextIO.write().to("src/main/java/resources/output/FileCombined.txt").withNumShards(1).withHeader("id|Name"));
        pipeline.run();
    }
}
