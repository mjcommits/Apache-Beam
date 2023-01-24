package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.values.PCollection;

public class ReadFromLocalFileSystemExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> lines = pipeline.apply(TextIO.read().from("src/main/java/resources/inputfile.csv"));
        lines.apply(TextIO.write().to("src/main/java/resources/outputfile.csv").withNumShards(1).withSuffix(".csv"));
        pipeline.run();
    }
}
