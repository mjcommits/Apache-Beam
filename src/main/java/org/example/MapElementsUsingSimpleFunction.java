package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

import java.util.regex.Pattern;

class User extends SimpleFunction<String, String> {

    @Override
    public String apply(String input) {
        String output = null;
        String[] values = input.split(Pattern.quote("|"));
        String name = values[0];
        String sex = values[1];
        if (sex.equals("1")) {
            output = name + "|" + "M";
        } else if (sex.equals("2")) {
            output = name + "|" + "F";
        } else {
            output = input;
        }
        return output;
    }
}

public class MapElementsUsingSimpleFunction {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> lines = pipeline.apply(TextIO.read().from("src/main/java/resources/InputfileColumns.csv"));
        PCollection<String> linesTransformingSex = lines.apply(MapElements.via(new User()));
        linesTransformingSex.apply("Writing file", TextIO.write().to("src/main/java/resources/output/SexTransformedColumns.csv").withNumShards(1).withSuffix("csv"));
        pipeline.run();
    }
}
