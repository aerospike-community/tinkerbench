package com.aerospike;

import org.apache.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.GremlinLangScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.codehaus.groovy.jsr223.GroovyScriptEngineImpl;

import javax.script.*;

public final class EvalQueryWorkloadProvider extends QueryWorkloadProvider {

    final String ScriptingEngine = "Groovy";

    final ScriptEngineManager manager;
    final GroovyScriptEngineImpl engine;
    final Bindings bindings;
    final String gremlinQuery;
    final String traversalSource;
    final LogSource logger;
    final Boolean isPrintResult;
    GremlinLangScriptEngine gremlinLangEngine = null;
    Bytecode gremlinEvalBytecode = null;

    public EvalQueryWorkloadProvider(WorkloadProvider provider,
                                     AGSGraphTraversal ags,
                                     String gremlinQueryScript) {
        super(provider, ags);

        final String[] parts = gremlinQueryScript.split("\\.");
        logger = getLogger();
        this.gremlinQuery = gremlinQueryScript.replace("'","\"");
        this.traversalSource = parts[0];
        isPrintResult = isPrintResult();

        System.out.printf("Preparing Gremlin traversal string with Source \"%s\":\n\t%s\n",
                            traversalSource,
                            gremlinQuery);

        logger.PrintDebug("EvalQueryWorkloadProvider", "Creating ScriptEngineManager for Source \"%s\" using Query \"%s\"",
                                    traversalSource,
                                    gremlinQuery);
        manager = new ScriptEngineManager();
        logger.PrintDebug("EvalQueryWorkloadProvider", String.format("Getting %s Engine", ScriptingEngine));
        //engine = manager.getEngineByName(ScriptingEngine);

        engine = new GroovyScriptEngineImpl();

        if (engine == null) {
            final String errMsg = String.format("%s Engine not found \"%s\"",
                                                    ScriptingEngine,
                                                    gremlinQuery);
            System.err.printf("Error: %s Engine not found. Shutting Down...\n",
                                ScriptingEngine);
            logger.error(errMsg);
            getOpenTelemetry().addException("Not Found", errMsg);
            provider.getCliArgs()
                    .abortRun.set(true);
            bindings = null;
            GetEngines(manager);
            return;
        }

        logger.PrintDebug("EvalQueryWorkloadProvider", "Binding to g");
        bindings = engine.createBindings();
        bindings.put(traversalSource, G());

        try {
            logger.PrintDebug("EvalQueryWorkloadProvider", "Generating Bytecode for \"%s\"", gremlinQuery);

            gremlinEvalBytecode = ((DefaultGraphTraversal<?, ?>) engine.eval(gremlinQuery, bindings)).getBytecode();

            logger.PrintDebug("EvalQueryWorkloadProvider",
                                "Generated Gremlin Bytecode: %s",
                                    gremlinEvalBytecode.toString());

            logger.PrintDebug("EvalQueryWorkloadProvider",
                            "Creating GremlinLangScriptEngine...");
            gremlinLangEngine = new GremlinLangScriptEngine();
        } catch (ScriptException e) {
            System.err.printf("ERROR: could not evaluate gremlin script \"%s\". Error: %s\n",
                    gremlinQuery,
                    e.getMessage());
            logger.error(String.format("ERROR: could not evaluate gremlin script \"%s\". Error: %s\n",
                            gremlinQuery,
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

    private static void GetEngines(ScriptEngineManager manager) {
        for (ScriptEngineFactory factory : manager.getEngineFactories()) {
            System.out.println("Engine Name: " + factory.getEngineName());
            System.out.println("Language Name: " + factory.getLanguageName());
            System.out.println("Names: " + factory.getNames());
            System.out.println("--------------------");
        }
    }

    @Override
    public WorkloadTypes WorkloadType() {
        return WorkloadTypes.QueryString;
    }

    @Override
    public String Name() {
        return "Query String";
    }

    /**
     * @return the Query
     */
    @Override
    public String getDescription() {
        return this.gremlinQuery;
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
