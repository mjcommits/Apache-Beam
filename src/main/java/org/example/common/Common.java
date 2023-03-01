package org.example.common;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.example.Options.MyOptions;

public class Common {

    public static MyOptions getPipelineOptions(String[] args){
        return PipelineOptionsFactory.fromArgs(args).withValidation().as(MyOptions.class);
    }
}
