package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.TypeDescriptors;

public class FilterTransformationExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        pipeline.apply(TextIO.read().from("src/main/java/resources/CustomerFile.txt"))
                .apply(MapElements.via(new MapInputFileToObject.CustomerMapper()))
                .apply(Filter.by(new FilterCustomer()))
                .apply(MapElements.into(TypeDescriptors.strings()).via(obj -> obj.getId()))
                .apply(TextIO.write().to("src/main/java/resources/output/OutputIDFiltered.txt").withNumShards(1));
        pipeline.run();
    }
}

class FilterCustomer implements SerializableFunction<Customer, Boolean> {

    @Override
    public Boolean apply(Customer customer) {
        return customer.getName().equals("Mona");
    }
}
