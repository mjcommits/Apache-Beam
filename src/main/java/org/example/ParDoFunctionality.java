package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.TypeDescriptors;

public class ParDoFunctionality {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        pipeline.apply(TextIO.read().from("src/main/java/resources/CustomerFile.txt"))
                .apply(MapElements.via(new MapInputFileToObject.CustomerMapper()))
                .apply(ParDo.of(new FilterNameOfCustomer()))
                .apply(MapElements.into(TypeDescriptors.strings()).via(obj -> obj.getId()))
                .apply(TextIO.write().to("src/main/java/resources/output/OutputIDFiltered.txt").withNumShards(1));
        pipeline.run();
    }
}

class FilterNameOfCustomer extends DoFn<Customer, Customer> {

    @ProcessElement
    public void processElement(ProcessContext c) {
        Customer input = c.element();
        if (input.getName().equals("Mona")) {
            c.output(input);
            System.out.println("Inside filtering cust" + input);
        }
    }

}
