package org.example;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Partition;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.example.DTO.Customer;
import org.example.Options.MyOptions;

public class PartitionTransformExample {
    public static void main(String[] args) {
        MyOptions options= PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
        Pipeline pipeline = Pipeline.create();
        PCollection<Customer> customerPCollection = pipeline.apply(TextIO.read().from(options.getInputFilePath()+"PartitionFile"))
                .apply(MapElements.via(new MapInputFileToObjectExample.CustomerMapper()));
        PCollectionList<Customer> pCollectionList = customerPCollection.apply(Partition.of(3, new DoPartition()));
        PCollection<Customer> fetchedPCollection = pCollectionList.get(0);
        fetchedPCollection.apply(MapElements.into(TypeDescriptors.strings()).via(obj -> obj.getName()))
                          .apply(TextIO.write().to("src/main/java/resources/output/PartitionFile0").withNumShards(1));
        pipeline.run();
    }

    private static class DoPartition implements Partition.PartitionFn<Customer> {

        @Override
        public int partitionFor(Customer customer,int num){
            if (customer.getName().equals("Mona")){
                return 0;
            } else if (customer.getName().equals("Nora")){
                return 1;
            } else {
                return 2;
            }
        }
    }
}
