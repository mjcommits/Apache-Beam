package org.example.Options;


import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptions;

public interface MyOptions extends PipelineOptions {

    void setInputFilePath(String filePath);

    @Default.String("src/main/java/resources/input/")
    String getInputFilePath();

    void setOutputFilePath(String filePath);

    @Default.String("src/main/java/resources/output/")
    String getOutputFilePath();

    void setExtn(String extn);

    @Default.String(".txt")
    String getExtn();
}
