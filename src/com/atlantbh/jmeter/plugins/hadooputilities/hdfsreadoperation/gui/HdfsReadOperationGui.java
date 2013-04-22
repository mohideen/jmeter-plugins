/*!
 * AtlantBH Custom Jmeter Components v1.0.0
 * http://www.atlantbh.com/jmeter-components/
 *
 * Copyright 2011, AtlantBH
 *
 * Licensed under the under the Apache License, Version 2.0.
 */
package com.atlantbh.jmeter.plugins.hadooputilities.hdfsreadoperation.gui;

import java.awt.BorderLayout;
import javax.swing.BorderFactory;
import org.apache.jmeter.gui.util.VerticalPanel;
import org.apache.jmeter.samplers.gui.AbstractSamplerGui;
import org.apache.jmeter.testelement.TestElement;
import org.apache.jorphan.gui.JLabeledTextField;

import com.atlantbh.jmeter.plugins.hadooputilities.hdfsreadoperation.HdfsReadOperation;
import kg.apc.jmeter.JMeterPluginsUtils;

/**
 * Java class representing GUI for the HDFS Operations component in JMeter
 *
 * @author Bakir Jusufbegovic / AtlantBH
 */
public class HdfsReadOperationGui extends AbstractSamplerGui {

    private static final long serialVersionUID = 1L;
    private JLabeledTextField inputFilePathTextField = null;
    private JLabeledTextField outputFilePathTextField = null;
    private JLabeledTextField nameNodeTextField = null;
    private static final String WIKIPAGE = "HDFSOperations";

    public HdfsReadOperationGui() {
        super();
        init();
    }

    public void init() {
        setLayout(new BorderLayout());
        setBorder(makeBorder());
        add(JMeterPluginsUtils.addHelpLinkToPanel(makeTitlePanel(), WIKIPAGE), BorderLayout.NORTH);

        VerticalPanel panel = new VerticalPanel();
        panel.setBorder(BorderFactory.createEtchedBorder());
        nameNodeTextField = new JLabeledTextField("Namenode");
        inputFilePathTextField = new JLabeledTextField("Input file destination in HDFS");
        outputFilePathTextField = new JLabeledTextField("Output destination");

        panel.add(nameNodeTextField);
        panel.add(inputFilePathTextField);
        panel.add(outputFilePathTextField);
        add(panel, BorderLayout.CENTER);
    }

    @Override
    public void clearGui() {
        super.clearGui();
        nameNodeTextField.setText("");
        inputFilePathTextField.setText("");
        outputFilePathTextField.setText("");
    }

    @Override
    public TestElement createTestElement() {
        HdfsReadOperation operations = new HdfsReadOperation();
        modifyTestElement(operations);
        operations.setComment(JMeterPluginsUtils.getWikiLinkText(WIKIPAGE));
        return operations;
    }

    @Override
    public String getLabelResource() {
        return this.getClass().getSimpleName();
    }

    @Override
    public String getStaticLabel() {
        return JMeterPluginsUtils.prefixLabel("HDFS Read Operation Sampler");
    }

    @Override
    public void modifyTestElement(TestElement element) {
        super.configureTestElement(element);
        if (element instanceof HdfsReadOperation) {
          HdfsReadOperation operations = (HdfsReadOperation) element;
            operations.setNameNode(nameNodeTextField.getText());
            operations.setInputFilePath(inputFilePathTextField.getText());
            operations.setOutputFilePath(outputFilePathTextField.getText());
        }
    }

    @Override
    public void configure(TestElement element) {
        super.configure(element);
        if (element instanceof HdfsReadOperation) {
          HdfsReadOperation operations = (HdfsReadOperation) element;
            nameNodeTextField.setText(operations.getNameNode());
            inputFilePathTextField.setText(operations.getInputFilePath());
            outputFilePathTextField.setText(operations.getOutputFilePath());
        }
    }
}
