package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.View;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionView;

import java.util.Map;
import java.util.regex.Pattern;

public class SideInputExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<KV<String, String>> pCollectionKV = pipeline.apply(TextIO.read().from("src/main/java/resources/CustomerReplaced.txt"))
                .apply(ParDo.of(new DoFn<String, KV<String, String>>() {
                    @ProcessElement
                    public void process(ProcessContext context) {
                        String input = context.element();
                        String args[] = input.split(Pattern.quote("|"));
                        KV<String, String> output = KV.of(args[0], args[1]);
                        context.output(output);
                    }
                }));

        PCollectionView<Map<String, String>> pView = pCollectionKV.apply(View.asMap());

        PCollection<String> custPColl = pipeline.apply(TextIO.read().from("src/main/java/resources/CustomerFile.txt"));
        custPColl.apply(ParDo.of(new DoFn<String, Void>() {
            @ProcessElement
            public void process(ProcessContext c) {
                Map<String, String> map = c.sideInput(pView);
                String input = c.element();
                String args[] = input.split(Pattern.quote("|"));
                if (map.get(args[0]) == null) {
                    System.out.println("Who not returned" + args[1]);
                }
            }
        }).withSideInputs(pView));
        pipeline.run();
    }
}
