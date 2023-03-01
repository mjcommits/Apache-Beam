package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.example.DTO.Customer;

import java.util.ArrayList;
import java.util.List;

public class InMemoryExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        //list<Object> converted to PCollection List
        PCollection<Customer> customerPCollection = pipeline.apply(Create.of(getCustomers()));
        //PCollection<Object> converted PCollection<Object> to PCollection<String>
        PCollection<String> customerStringPCollection = customerPCollection.apply(MapElements.into(TypeDescriptors.strings()).via(Customer::getName));
        //Element wise transformation example
        PCollection<String> capitalListPCollection = customerStringPCollection.apply(MapElements.into(TypeDescriptors.strings()).via(obj -> obj.toUpperCase()));
        capitalListPCollection.apply(TextIO.write().to("src/main/java/resources/customerIDList.csv").withNumShards(1).withSuffix(".csv"));
        pipeline.run();
    }
    public static List<Customer> getCustomers(){
        Customer customer1 =new Customer("1","Mona");
        Customer customer2=new Customer("2", "Nora");
        List<Customer> customerList = new ArrayList<>();
        customerList.add(customer1);
        customerList.add(customer2);
        return customerList;
    }
}
