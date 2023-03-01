package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.example.DTO.Customer;
import org.example.Options.MyOptions;
import org.example.common.Common;

/**
 * Filter and reduce PCollection using SerializableFunction which return true/false
 */
public class FilterExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        MyOptions options = Common.getPipelineOptions(args);
        pipeline.apply(TextIO.read().from(options.getInputFilePath()+"CustomerFile.txt"))
                .apply(MapElements.via(new MapInputFileToObjectExample.CustomerMapper()))
                .apply(Filter.by(new FilterCustomer()))
                .apply(MapElements.into(TypeDescriptors.strings()).via(obj -> obj.getId()))
                .apply(TextIO.write().to(options.getOutputFilePath()+"OutputIDFiltered.txt").withNumShards(1));
        pipeline.run();
    }
}

class FilterCustomer implements SerializableFunction<Customer, Boolean> {

    @Override
    public Boolean apply(Customer customer) {
        return customer.getName().equals("Mona");
    }
}
