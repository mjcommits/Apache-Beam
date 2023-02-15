package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.ArrayList;
import java.util.List;

public class DistinctExample {
    public static void main(String[] args) {
        Pipeline pipeline = Pipeline.create();
        PCollection<Customer> customerPCollection = pipeline.apply(Create.of(getCustomers()));
        PCollection<String> stringListPCollection = customerPCollection.apply(MapElements.into(TypeDescriptors.strings()).via(obj -> obj.toString()));
        PCollection<String> customerPCollectionDistinct = stringListPCollection.apply(Distinct.<String>create());
        customerPCollectionDistinct.apply(MapElements.into(TypeDescriptors.strings()).via(obj -> { System.out.println(obj);return obj;}));
        pipeline.run();
    }
    public static List<Customer> getCustomers(){
        Customer customer1 =new Customer("1","Mona");
        Customer customer2=new Customer("2", "Nora");
        Customer customer3=new Customer("2", "Nora");
        List<Customer> customerList = new ArrayList<>();
        customerList.add(customer1);
        customerList.add(customer2);
        customerList.add(customer3);
        return customerList;
    }
}
