package org.example.Options;

import org.apache.beam.sdk.options.PipelineOptions;

public interface MyOptions extends PipelineOptions {
    void setInputFilePath(String filePath);
    String getInputFilePath();

    void setOutputFilePath(String filePath);
    String getOutputFilePath();

    void setExtn(String extn);
    String getExtn();
}
