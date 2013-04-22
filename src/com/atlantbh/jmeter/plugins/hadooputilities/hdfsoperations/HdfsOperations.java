/*!
 * AtlantBH Custom Jmeter Components v1.0.0
 * http://www.atlantbh.com/jmeter-components/
 *
 * Copyright 2011, AtlantBH
 *
 * Licensed under the under the Apache License, Version 2.0.
 */
package com.atlantbh.jmeter.plugins.hadooputilities.hdfsoperations;

import java.io.File;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.jmeter.samplers.AbstractSampler;
import org.apache.jmeter.samplers.Entry;
import org.apache.jmeter.samplers.SampleResult;

/**
 * Java class that exposes functionality that this JMeter component offers.
 * Those functionalities are: 1. Copy file to HDFS from local machine
 *
 * @author Bakir Jusufbegovic / AtlantBH
 */
public class HdfsOperations extends AbstractSampler {

    private static final long serialVersionUID = 1L;
    private Configuration config = null;
    private FileSystem hdfs = null;
    private static final String INPUTPATH = "INPUTPATH";
    private static final String OUTPUTPATH = "OUTPUTPATH";
    private static final String UPLOADSUCCESS = "UPLOADSUCCESS";
    private static final String NAMENODE = "NAMENODE";
    private String extractedLine = "";

    public HdfsOperations() {
    }

    ;
	
	  public HdfsOperations(String nameNode) throws IOException {
    	  config = new Configuration();
        config.set("fs.default.name", nameNode);
        File coreSite = new File("/etc/hadoop/core-site.xml");
        File hdfsSite = new File("/etc/hadoop/hdfs-site.xml");
        if(coreSite.exists() && hdfsSite.exists()) {
          config.clear();
          config.addResource(new Path(coreSite.getPath()));
          config.addResource(new Path(hdfsSite.getPath()));
        }
        hdfs = FileSystem.get(config);
    }

    public void setExtractedLine(String extractedLine) {
        this.extractedLine = extractedLine;
    }

    public String getExtractedLine() {
        return extractedLine;
    }

    public void setNameNode(String nameNode) {
        setProperty(NAMENODE, nameNode);
    }

    public String getNameNode() {
        return getPropertyAsString(NAMENODE);
    }

    public void setUploadSuccess(String uploadSuccess) {
        setProperty(UPLOADSUCCESS, uploadSuccess);
    }

    public String getUploadSuccess() {
        return getPropertyAsString(UPLOADSUCCESS);
    }

    public void setInputFilePath(String inputFilePath) {
        setProperty(INPUTPATH, inputFilePath);
    }

    public String getInputFilePath() {
        return getPropertyAsString(INPUTPATH);
    }

    public void setOutputFilePath(String outputFilePath) {
        setProperty(OUTPUTPATH, outputFilePath);
    }

    public String getOutputFilePath() {
        return getPropertyAsString(OUTPUTPATH);
    }

    private void initializeConnection() throws IOException {
      config = new Configuration();
      config.set("fs.default.name", this.getNameNode());
      File coreSite = new File("/etc/hadoop/core-site.xml");
      File hdfsSite = new File("/etc/hadoop/hdfs-site.xml");
      if(coreSite.exists() && hdfsSite.exists()) {
        config.clear();
        config.addResource(new Path(coreSite.getPath()));
        config.addResource(new Path(hdfsSite.getPath()));
      }
      hdfs = FileSystem.get(config);
    }

    private void copyFileToHDFS() throws IOException {
        Path localPath = new Path(this.getInputFilePath());
        Path hdfsPath = new Path(this.getOutputFilePath());
        
        hdfs.create(hdfs, hdfsPath, new FsPermission("755"));

        String fileName = localPath.getName();
        Path hdfsFileLocation = new Path(this.getOutputFilePath() + "/" + fileName);
        
        try {
          hdfs.copyFromLocalFile(false, true, localPath, hdfsPath);
          this.setUploadSuccess("File " + fileName + " copied to HDFS on location: " + hdfsPath);
        } catch(IOException e) {
          throw e;
        }
        /*if (!hdfs.exists(hdfsFileLocation)) {
            hdfs.copyFromLocalFile(localPath, hdfsPath);
            this.setUploadSuccess("File " + fileName + " copied to HDFS on location: " + hdfsPath);
        } else {
            this.setUploadSuccess("File " + fileName + " already exists on HDFS on location: " + hdfsPath);
        }*/

        //hdfs.close();
    }

    @Override
    public SampleResult sample(Entry e) {
        SampleResult result = new SampleResult();

        String requestData = "Namenode: " + this.getNameNode() + "\n";
        requestData += "Input file path: " + this.getInputFilePath() + "\n";
        requestData += "Output file path: " + this.getOutputFilePath();

        result.setSampleLabel(getName());
        result.setSamplerData(requestData);
        result.setDataType(SampleResult.TEXT);

        result.sampleStart();

        if (!(this.getNameNode().equalsIgnoreCase("") || this.getInputFilePath().equalsIgnoreCase("") || this.getOutputFilePath().equalsIgnoreCase(""))) {
            try {
                this.initializeConnection();
                this.copyFileToHDFS();
                result.setResponseData(this.getUploadSuccess().getBytes());
                result.setSuccessful(true);
            } catch (IOException e1) {
                result.setResponseData(e1.getMessage().getBytes());
                e1.printStackTrace();
                result.setSuccessful(false);
            } catch (Exception e2) {
                result.setResponseData(e2.getMessage().getBytes());
                result.setSuccessful(false);
            }
        } else {
            result.setSuccessful(false);
            result.setResponseData("Some input fields are not populated".getBytes());
        }

        result.sampleEnd();
        return result;
    }
}
