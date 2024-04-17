### Writing a query

NRC queries are described natively in Scala using the NRC language defined in `src/main/scala/framework/nrc/NRC.scala`. 
A newly defined query should extend the Query trait (see Query.scala) to leverage various support functions for executing the 
stages of the pipeline. 

A Query (defined in `Query.scala`) is a trait that extends the components necessary 
to execute the pipeline for code generation. When writing a query, we will create 
an object that extends this. 

For writing an NRC query with the TraNCE CLI, users have the option to write directly within the CLI 
using commands or to open an external editor by invoking the `\query -editor` command.

#### Defining Query in CLI

To define a query inside the TraNCE CLI directly, without the need for an external editor, 
we simply execute the `[query_name] := [query_content]`; command. For instance, to define a query that retrieves the order 
date and order key from the Order table, the following command can be used within the CLI:
```
Query1 := 
for o in Order union
    {(date := o.o_orderdate, ok := o.o_orderkey)};
```

Remember to conclude the command with a semicolon to signal the end of the NRC query input. 
We can then proceed to the 'Query Evaluation' or 'Code Generation' sections to evaluate the 
query or generate the corresponding Spark application.

### Query Evaluation

Evaluating a query requires first loading the JSON tables it references. To evaluate Query1, for example, the following commands should be executed:
```
\load -table Order order.json
\evaluate Query1
```

The evaluation result will be displayed in the CLI. Optionally, we can save the evaluation result as a local JSON file by executing:
```
\evaluate Query1 to Query1.json
```
This command saves the Query1 evaluation result into a `Query1.json` file located in the `src/main/scala/shell/eval_results` directory.
### Code Generation

To generate a Spark program, we must specify both the generation pipeline and the query name in the command. 
An option is available to view the intermediate states of the query during Spark generation by calling `\generate -intermediate on` command before the generation to enable this functionality. 
For instance, to generate a Spark application for our example `Query1` using the standard pipeline, we should input:
```
\generate -standard Query1
```

The Spark application will be saved to `../executor/spark/src/main/scala/sparkutils/generated/`. We can then consult the 'Spark Execution' section for instructions on executing the generated Spark code.
### Execution

Executing the Spark program involves packaging the generated files into an executable JAR application and running the actual execution. 
Due to the extensive log messages produced during Spark execution, we provide an option to save the execution result and log messages 
to a local text file rather than displaying them in the CLI. To execute and save the results for `Query1`, 
we first use the `\show -generated command` to verify the Spark file's name, which is `Query1ProjSpark`. 
Then, execute the following command:

```
\execute -save Query1ProjSpark
```

This creates a `Query1ProjSpark.txt` file in the `/src/main/scala/shell/execution` folder, containing all log messages from the Apache Spark engine and the actual execution results for our example `Query1`.