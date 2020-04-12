# NiFi WarpScript Processor

This NiFi Processor enables your NiFi flows to execute WarpScript code to process your FlowFiles.

# Installation

The processor must be built from the source which reside on [GitHub](https://github.com/senx/nifi-warp10-processor).

## Cloning the git repository

This step will bring the processor source code onto your machine. Execute the following command:

```
git clone git@github.com:senx/nifi-warp10-processor.git
```

## Building the *NAR* file

NiFi Processors are packaged as `.nar` (NiFi Archive) files. The NiFi WarpScript Processor respects this convention and therefore the `.nar` file must be built before you can use the features it provides.

The build process is very simple, simply run the following command from the `nifi-warpscript-processor` directory created by the clone operation of the previous step.

```
./gradlew nar
```

This will produce a file named `nifi-warp10-processor.nar` in the `build/libs` subdirectory.

## Deployment and Configuration

### Deployment

Simply copy the `nifi-warp10-processor.nar` file produced above into the `lib` directory of your NiFi deployment.

### Configuration

The WarpScript Processor can be fully configured by specifying a Warp 10 configuration file in the `WARP10_CONFIG` environment variable or the `warp10.config` JVM property.

The configuration should at least specify the time units to use via the `warp.timeunits` configuration key.

If no configuration file is specified, the processor will assume the time units to be microseconds.

Once you have copied the `.nar` file and set the configuration, you can relaunch your NiFi instance via:

```
./bin/nifi.sh start
```

You can confirm that your WarpScript Processor is correctly deployed by adding a processor to your flow and checking that you see `WarpScriptProcessor` in the list.

Congratulations, your NiFi instance can now execute WarpScript code to process your FlowFiles!

# Usage

The `WarpScriptProcessor` can be inserted in your flows just like any other processor.

## Processor Node Configuration

WarpScriptProcessor nodes are configured using 3 properties:

* **Execution Semantics**

This property determines how the execution environment is managed between executions, *i.e.* between the processing of different FlowFiles. The possible values are `PERTHREAD`, the environment will be reused across calls within the same thread, `NEW`, a new environment will be created before each execution, and `SYNCHRONIZED` meaning a single environment will be used across calls and across threads. The default `PERTHREAD` should be used in most cases.

* **Max FlowFiles**

This property determines the maximum number of FlowFiles which can be processed at once by the WarpScriptProcessor. This defaults to 1.

* **WarpScript**

This is the actual WarpScript code to execute. This code will be run for each batch of FlowFiles (up to *Max FlowFiles*). The FlowFiles will be available to your code using the calling convention detailed below.

Note that the NiFi UI traps the press on the `ENTER` or `RETURN` key, so if you want to skip a line in your code, you should hold the `SHIFT` key down before pressing `ENTER`.

Note that modifying any of those properties will recreate the execution environments.

## Calling convention

For each batch of incoming FlowFiles, your WarpScript code is called with a list of FlowFiles as input.

Each FlowFile is represented as a map with the following elements:

```
entryDate         EntryDate in platform time units since the Unix Epoch
lineageStartDate  LineageStartDate in platform time units since the Unix Epoch
fileSize          FlowFile size in bytes
lastQueueDate     LastQueueDate in platform time units since the Unix Epoch
id                FlowFile id
lineageStartIndex FlowFile LineageStartIndex value
queueDateIndex    FlowFile QueueDateIndex
penalized         FlowFile penalized flag
attributes        Map of FlowFile attributes
content           FlowFile content as a byte array            
```

The FlowFiles are removed from the session prior to being processed by your WarpScript code.

The WarpScript code is expected to produce 0 or more maps as output (one per stack level), each map will be converted to a FlowFile.

Each of those maps can contain an `attribute` key with an associated map of attributes, and a `content` key with either a byte array or a STRING as value. The value associated with the `content` key will be the content of the resulting FlowFile, as is if it is a byte array, or its `UTF-8` byte representation if it is a STRING.

If the execution encountered an error, the error message will be passed in a FlowFile down the ERROR relationship, otherwise the generated FlowFiles will be passed down the SUCCESS relationship.
