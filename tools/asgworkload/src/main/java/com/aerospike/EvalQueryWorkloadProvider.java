package com.aerospike;

import org.apache.tinkerpop.gremlin.jsr223.GremlinLangScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;

import javax.script.*;

public final class EvalQueryWorkloadProvider extends QueryWorkloadProvider {

    final ScriptEngineManager manager;
    final GremlinLangScriptEngine engine;
    final Bindings bindings;
    final String gremlinString;
    final String traversalSource;
    final LogSource logger;
    final Boolean isPrintResult;
    GremlinLangScriptEngine gremlinLangEngine = null;
    Bytecode gremlinEvalBytecode = null;

    public EvalQueryWorkloadProvider(WorkloadProvider provider,
                                     AGSGraphTraversal ags,
                                     String gremlinScript) {
        super(provider, ags, gremlinScript);

        final String[] parts = gremlinScript.split("\\.");
        logger = getLogger();
        this.gremlinString = gremlinScript.replace("'","\"");
        this.traversalSource = parts[0];
        isPrintResult = isPrintResult();

        System.out.printf("Preparing Gremlin traversal string with Source \"%s\":\n\t%s for %s\n",
                            traversalSource,
                            gremlinString,
                            isWarmup() ? "Warmup" : "Workload");

        logger.PrintDebug("EvalQueryWorkloadProvider", "Creating ScriptEngineManager for Source \"%s\" using Query \"%s\"",
                                    traversalSource,
                                    gremlinString);
        manager = new ScriptEngineManager();
        logger.PrintDebug("EvalQueryWorkloadProvider", "Getting GremlinLangScriptEngine Engine");
        engine = new GremlinLangScriptEngine();

        logger.PrintDebug("EvalQueryWorkloadProvider", "Binding to g");
        bindings = engine.createBindings();
        bindings.put(traversalSource, G());

        try {
            logger.PrintDebug("EvalQueryWorkloadProvider", "Generating Bytecode for \"%s\"", gremlinString);

            gremlinEvalBytecode = ((DefaultGraphTraversal<?, ?>) engine.eval(gremlinString, bindings)).getBytecode();

            logger.PrintDebug("EvalQueryWorkloadProvider",
                                "Generated Gremlin Bytecode: %s",
                                    gremlinEvalBytecode.toString());

            logger.PrintDebug("EvalQueryWorkloadProvider",
                            "Creating GremlinLangScriptEngine...");
            gremlinLangEngine = new GremlinLangScriptEngine();
        } catch (ScriptException e) {
            System.err.printf("ERROR: could not evaluate gremlin script \"%s\". Error: %s\n",
                    gremlinString,
                    e.getMessage());
            logger.error(String.format("ERROR: could not evaluate gremlin script \"%s\". Error: %s\n",
                            gremlinString,
                            e.getMessage()),
                        e);
            getOpenTelemetry().addException(e);
            provider.getCliArgs()
                    .abortRun.set(true);
        }

        logger.PrintDebug("EvalQueryWorkloadProvider",
                        "%s...",
                            provider.getCliArgs().abortRun.get()
                                ? "Aborted"
                                : "Completed");
    }

    /*private static void GetEngines(ScriptEngineManager manager) {
        for (ScriptEngineFactory factory : manager.getEngineFactories()) {
            System.out.println("Engine Name: " + factory.getEngineName());
            System.out.println("Language Name: " + factory.getLanguageName());
            System.out.println("Names: " + factory.getNames());
            System.out.println("--------------------");
        }
    }*/

    @Override
    public WorkloadTypes WorkloadType() {
        return WorkloadTypes.GremlinString;
    }

    /**
     * @return the Query
     */
    @Override
    public String getDescription() {
        return "Executes an user defined Gremlin String";
    }

    @Override
    public boolean preProcess() throws InterruptedException {
        return true;
    }

    @Override
    public void postProcess() {
    }

    @Override
    public Boolean call() throws Exception {
        try (final Traversal.Admin<?,?> resultTraversal = gremlinLangEngine.eval(gremlinEvalBytecode,
                                                                                    bindings,
                                                                                    traversalSource)) {
            if(isPrintResult) {
                resultTraversal.forEachRemaining(this::PrintResult);
            }
        }
        return true;
    }
}
