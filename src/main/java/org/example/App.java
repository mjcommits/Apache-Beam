package org.example;

import org.apache.beam.sdk.Pipeline;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        Pipeline pipeline = Pipeline.create();
        pipeline.run();
        System.out.println( "Pipeline run successfully!" );
    }
}
