//
//   Copyright 2019  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//

package io.warp10.nifi;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.warp10.Revision;
import io.warp10.WarpConfig;
import io.warp10.continuum.Configuration;
import io.warp10.continuum.store.Constants;
import io.warp10.script.WarpScriptException;
import io.warp10.script.WarpScriptExecutor;
import io.warp10.script.WarpScriptExecutor.StackSemantics;
import io.warp10.script.functions.REPORT;

public class WarpScriptProcessor extends AbstractProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(WarpScriptProcessor.class);
  
  private List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
  private Set<Relationship> relationships = new HashSet<Relationship>();
  
  private final Map<String,Object> DEFAULT_SYMBOLS = new HashMap<String,Object>();
  
  private static final String SYMBOL_PREFIX = ".nifi.";
  private static final String CONTENT_KEY = "content";
  private static final String ATTRIBUTES_KEY = "attributes";
  
  private WarpScriptExecutor executor = null;
  private StackSemantics semantics = StackSemantics.PERTHREAD;
  private String script = "";
  
  //
  // WarpScript™ code to execute
  //
  
  private PropertyDescriptor WARPSCRIPT = new PropertyDescriptor
    .Builder().name("WARPSCRIPT")
    .displayName("WarpScript")
    .description("WarpScript™ code to execute on current node")
    .defaultValue("")      
    .expressionLanguageSupported(ExpressionLanguageScope.NONE)
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();
 
  //
  // Define WarpScript executor semantics
  // Execution environment can be per thread, per execution or synchronized across threads
  //
  
  private PropertyDescriptor SEMANTICS = new PropertyDescriptor
    .Builder().name("SEMANTICS")
    .displayName("Execution Semantics")
    .description("Define how the execution environment is handled between executions")
    .defaultValue(StackSemantics.PERTHREAD.name())
    .allowableValues(StackSemantics.PERTHREAD.name(), StackSemantics.SYNCHRONIZED.name(), StackSemantics.NEW.name())
    .required(true)
    .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
    .build();
    
  private PropertyDescriptor MAXFLOWFILES = new PropertyDescriptor
    .Builder().name("MAXFLOWFILES")
    .displayName("Max FlowFiles")
    .defaultValue("1")
    .description("Maximum number of FlowFiles to process per execution")
    .required(true)
    .addValidator(StandardValidators.POSITIVE_INTEGER_VALIDATOR)
    .build();
  
  private Relationship SUCCESS = new Relationship.Builder()
    .name("SUCCESS")
    .description("Success relationship")
    .autoTerminateDefault(true)
    .build();
  
  private Relationship FAILURE = new Relationship.Builder()
    .name("FAILURE")
    .description("Failure relationship")
    .autoTerminateDefault(true)
    .build();


  @Override
  protected void init(ProcessorInitializationContext context) {
    
    //
    // Initialize Warp 10™ configuration
    //
  
    try {
      loadConfig();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
    
    //
    // Create the descriptors for the NiFi UI
    //
    
    this.descriptors = new ArrayList<PropertyDescriptor>();
    descriptors.add(WARPSCRIPT);
    descriptors.add(SEMANTICS);
    descriptors.add(MAXFLOWFILES);
    
    //
    // Set relationships
    //
    
    this.relationships = new HashSet<Relationship>();
    relationships.add(SUCCESS);
    relationships.add(FAILURE);
    
    //
    // Initialize WarpScriptExecutor
    //
    
    this.semantics = StackSemantics.valueOf(SEMANTICS.getDefaultValue());
    this.script = WARPSCRIPT.getDefaultValue();
    
    try {
      this.executor = new WarpScriptExecutor(semantics, script, DEFAULT_SYMBOLS);
    } catch (WarpScriptException e) {
      throw new RuntimeException(e);
    }
  }
  
  @Override
  public void onPropertyModified(PropertyDescriptor descriptor, String oldValue, String newValue) {

    boolean newExecutor = false;
    
    //
    // Properties beginning with SYMBOL_PREFIX are converted to symbols passed to the executor
    //
    
    if (descriptor.getName().equals(WARPSCRIPT.getName())) {
      this.script = newValue;
      newExecutor = true;
    } else if (descriptor.getName().equals(SEMANTICS.getName())) {
      this.semantics = StackSemantics.valueOf(newValue);
      newExecutor = true;
    } else {
      this.executor.store(SYMBOL_PREFIX + descriptor.getName(), newValue);
    }
      
    if (newExecutor) {
      try {
        this.executor = new WarpScriptExecutor(this.semantics, this.script, this.DEFAULT_SYMBOLS);
      } catch (WarpScriptException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  @Override
  public Set<Relationship> getRelationships() {
    return this.relationships;
  }  
  
  @Override
  protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
    return this.descriptors;
  }  
  
  @Override
  public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
    try {
      //
      // Retrieve the maximum number of FlowFiles to process
      //
      
      int nfiles = context.getProperty(MAXFLOWFILES).asInteger();    
      List<FlowFile> flowfiles = session.get(nfiles);
      
      //
      // Generate the input parameters passed to the executor
      //
      
      List<Object> params = new ArrayList<Object>();
        
      for (FlowFile flowFile: flowfiles) {        
        if(null != flowFile) {                   
          //
          // A map of the current flow file properties
          //

          Map<String, Object> props = new HashMap<String, Object>();  
          props.put("entryDate", flowFile.getEntryDate() * Constants.TIME_UNITS_PER_MS);
          props.put("lineageStartDate", flowFile.getLineageStartDate() * Constants.TIME_UNITS_PER_MS);
          props.put("fileSize", flowFile.getSize());
          props.put("lastQueueDate", flowFile.getLastQueueDate() * Constants.TIME_UNITS_PER_MS);
          props.put("id", flowFile.getId());
          props.put("lineageStartIndex", flowFile.getLineageStartIndex());
          props.put("queueDateIndex", flowFile.getQueueDateIndex());
          props.put("penalized", flowFile.isPenalized());
            
          props.put(ATTRIBUTES_KEY, new HashMap<String, String>(flowFile.getAttributes()));
            
          ByteArrayOutputStream out = new ByteArrayOutputStream((int) flowFile.getSize());
          session.exportTo(flowFile, out);
            
          props.put(CONTENT_KEY, out.toByteArray());
            
          params.add(props);
        }
      }
      
      session.remove(flowfiles);

      //
      // Execute WarpScript™ code and store results
      //
            
      List<Object> results = null;
      
      Throwable error = null;
      
      try {
        results = this.executor.exec(params);
        // Reverse the list so the deepest levels are handled first
        Collections.reverse(results);
      } catch (Throwable t) {
        error = t;
      }
      
      List<FlowFile> output = new ArrayList<FlowFile>();
      
      if (null == error) {   
        //
        // For each result returned by the WarpScript code, generate a new Flow file
        // 
        
        for (Object result: results) {
          
          if (!(result instanceof Map)) {
            throw new ProcessException("Invalid return value, expected a MAP.");
          }
          
          Map<Object,Object> map = (Map<Object,Object>) result;
          
          //
          // Create flow file
          //
          
          FlowFile flowFile = session.create();

          if (map.containsKey(ATTRIBUTES_KEY)) {
            if (!(map.get(ATTRIBUTES_KEY) instanceof Map)) {
              throw new ProcessException("Invalid content for key '" + ATTRIBUTES_KEY + "', expected a MAP.");
            }
            
            for (Entry<Object,Object> entry: ((Map<Object,Object>) map.get(ATTRIBUTES_KEY)).entrySet()) {
              session.putAttribute(flowFile, entry.getKey().toString(), entry.getValue().toString());
            }
          }
          
          if (map.containsKey(CONTENT_KEY)) {
            byte[] content;
            if (map.get(CONTENT_KEY) instanceof byte[]) {
              content = (byte[]) map.get(CONTENT_KEY);
            } else {
              content = map.get(CONTENT_KEY).toString().getBytes("UTF-8");
            }
            
            OutputStream out = session.write(flowFile);
            out.write(content);
            out.close();
          }
          
          output.add(flowFile);
        }      
      } else {      
        //
        // When an error occurred generate a fail relationship containing the error message
        //
        
        FlowFile flowFile = session.create();
        
        OutputStream out = session.write(flowFile);
        out.write(error.getMessage().getBytes("UTF-8"));
        out.close();
        
        output.add(flowFile);
      }
        
      //
      // Transfer current session file to the following NiFi element
      //    
      
      session.transfer(output, null == error ? SUCCESS : FAILURE);
      session.commit();
      
    } catch (ProcessException pe) {
      throw pe;
    } catch (Throwable t) {
      throw new ProcessException(t);
    }    
  }
  
  private void loadConfig() throws IOException {
    //
    // Extract the 
    String config = System.getenv(WarpConfig.WARP10_CONFIG_ENV);
    
    if (null == config) {
      config = System.getProperty(WarpConfig.WARP10_CONFIG);
    }
    
    if (null == config) {
      StringBuilder sb = new StringBuilder();
      sb.append("warp.timeunits = us\n");
      sb.append("warpfleet.macros.repos = https://warpfleet.senx.io/macros\n");
      StringReader reader = new StringReader(sb.toString());
      WarpConfig.safeSetProperties(reader);
    } else {
      WarpConfig.safeSetProperties(config);
    }
    
    
    LOG.info("\n" + Constants.WARP10_BANNER + "\n" + "Revision: " + Revision.REVISION + "\n");
  }    
}
