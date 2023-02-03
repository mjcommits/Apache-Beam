package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.PCollection;

import java.util.regex.Pattern;

public class MapInputFileToObject {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<String> linesOfFilePCollection = pipeline.apply(TextIO.read().from("src/main/java/resources/CustomerFile.txt"));
        PCollection<Customer> customerPCollection =linesOfFilePCollection.apply(MapElements.via(new CustomerMapper()));
        customerPCollection.apply(MapElements.via(new PrintEachCustomer()));
        pipeline.run();
    }

    static class CustomerMapper extends SimpleFunction<String,Customer> {

        @Override
        public Customer apply(String input) {
            String[] lineValues = input.split(Pattern.quote("|"));
            Customer customer = new Customer(lineValues[0],lineValues[1]);
            return customer;
        }
    }

    private static class PrintEachCustomer extends SimpleFunction<Customer,Customer> {

        @Override
        public Customer apply(Customer customer) {
            System.out.println(customer);
            return customer;
        }
    }
}
