package com.aerospike;

import org.apache.tinkerpop.gremlin.jsr223.GremlinLangScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;

import javax.script.*;

public final class EvalQueryWorkloadProvider extends QueryWorkloadProvider {

    final GremlinLangScriptEngine engine;
    final Bindings bindings;
    final String gremlinString;
    final String traversalSource;
    final LogSource logger;
    final Boolean isPrintResult;
    GremlinLangScriptEngine gremlinLangEngine = null;
    Bytecode gremlinEvalBytecode = null;
    Terminator terminator = Terminator.toList;
    boolean requiresIdFormat = false;
    final IdManager idManager;
    ThreadLocal<Bytecode> bytecodeThreadLocal;

    enum Terminator {
        next,
        toList,
        iterate,
        toSet
    }

    public EvalQueryWorkloadProvider(WorkloadProvider provider,
                                     AGSGraphTraversal ags,
                                     String gremlinScript,
                                     IdManager idManager) {
        super(provider, ags, gremlinScript);
        this.idManager = idManager;

        if (gremlinScript.endsWith("next()") ||
            gremlinScript.endsWith("toList()") ||
            gremlinScript.endsWith("iterate()") ||
            gremlinScript.endsWith("toSet()")) {
            // Remove the terminator from the script
            String terminatorString = gremlinScript.substring(gremlinScript.lastIndexOf('.') + 1);
            switch (terminatorString) {
                case "next()":
                    terminator = Terminator.next;
                    break;
                case "toList()":
                    terminator = Terminator.toList;
                    break;
                case "iterate()":
                    terminator = Terminator.iterate;
                    break;
                case "toSet()":
                    terminator = Terminator.toSet;
                    break;
                default:
                    throw new IllegalArgumentException(String.format("Error Unknown terminator \"%s\" in Gremlin script \"%s\"\n",
                            terminatorString,
                            gremlinScript));
            }
            gremlinScript = gremlinScript.substring(0, gremlinScript.lastIndexOf('.'));
        }
        if (gremlinScript.contains("%s")) {
            requiresIdFormat = true;
        }

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
        logger.PrintDebug("EvalQueryWorkloadProvider", "Getting GremlinLangScriptEngine Engine");
        engine = new GremlinLangScriptEngine();

        logger.PrintDebug("EvalQueryWorkloadProvider", "Binding to g");
        bindings = engine.createBindings();
        bindings.put(traversalSource, G());

        try {
            logger.PrintDebug("EvalQueryWorkloadProvider", "Generating Bytecode for \"%s\"", gremlinString);

            Object id = idManager.getId();
            if (id instanceof String) {
                id = "'" + id + "'";
            }
            gremlinEvalBytecode = ((DefaultGraphTraversal<?, ?>) engine.eval(String.format(gremlinString, id), bindings)).getBytecode();

            logger.PrintDebug("EvalQueryWorkloadProvider",
                                "Generated Gremlin Bytecode: %s",
                                    gremlinEvalBytecode.toString());

            logger.PrintDebug("EvalQueryWorkloadProvider",
                            "Creating GremlinLangScriptEngine...");
            gremlinLangEngine = new GremlinLangScriptEngine();
            Object finalId = id;
            bytecodeThreadLocal = ThreadLocal.withInitial(() -> {
                try {
                    return ((DefaultGraphTraversal<?, ?>) engine.eval(String.format(gremlinString, finalId), bindings)).getBytecode();
                } catch (ScriptException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
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
       //TODO: This needs to be refactored moving the close outside te scope of the measurement...
        if (requiresIdFormat) {
            final Object id = idManager.getId();
            Object[] data = bytecodeThreadLocal.get().getStepInstructions().getFirst().getArguments();
            data[0] = id;
        }
        // If close not performed, there seems to be a leak according to the profiler
        try (final Traversal.Admin<?,?> resultTraversal = gremlinLangEngine.eval(bytecodeThreadLocal.get(),
                                                                                    bindings,
                                                                                    traversalSource)) {
            if (isPrintResult) {
                resultTraversal.forEachRemaining(this::PrintResult);
            } else {
                switch (terminator) {
                    case next:
                        resultTraversal.next();
                        break;
                    case iterate:
                        resultTraversal.iterate();
                        break;
                    case toSet:
                        resultTraversal.toSet();
                        break;
                    case toList:
                        resultTraversal.toList();
                        break;
                    default:
                        throw new IllegalStateException("This should never happen: Unknown terminator " + terminator);
                }
            }
        }
        return true;
    }
}
