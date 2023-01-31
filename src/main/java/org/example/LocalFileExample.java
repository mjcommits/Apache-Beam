package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.values.PCollection;
import org.example.Options.MyOptions;

public class LocalFileExample {
    public static void main(String[] args) {
        MyOptions options= PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        Pipeline pipeline = Pipeline.create();
        PCollection<String> lines = pipeline.apply(TextIO.read().from(options.getInputFilePath()));
        lines.apply(TextIO.write().to(options.getOutputFilePath()).withNumShards(1).withSuffix(options.getExtn()));
        pipeline.run();
    }
}
