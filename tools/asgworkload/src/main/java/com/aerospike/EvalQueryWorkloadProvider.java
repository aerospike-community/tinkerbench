package com.aerospike;

import groovy.transform.Final;
import org.apache.tinkerpop.gremlin.jsr223.GremlinLangScriptEngine;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.javatuples.Pair;

import javax.script.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    final Pattern funcPattern = Pattern.compile("^\\s*(?<stmt>.+)\\.(?<func>[^(]+)\\(\\s*\\)\\s*$");

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

        Matcher matcher = funcPattern.matcher(gremlinScript);
        if (matcher.find()) {
            String terminatorString = matcher.group("func").toLowerCase();
            switch (terminatorString) {
                case "next":
                    terminator = Terminator.next;
                    break;
                case "tolist":
                    terminator = Terminator.toList;
                    break;
                case "iterate":
                    terminator = Terminator.iterate;
                    break;
                case "toset":
                    terminator = Terminator.toSet;
                    break;
                default:
                    throw new IllegalArgumentException(String.format("Error Unknown terminator function \"%s\" in Gremlin script \"%s\"\n",
                            terminatorString,
                            gremlinScript));
            }
            gremlinScript = matcher.group("stmt");
        }
        if (gremlinScript.contains("%s")) {
            requiresIdFormat = true;
        }

        final String[] parts = gremlinScript.split("\\.");
        logger = getLogger();
        this.gremlinString = gremlinScript.replace("'","\"");
        this.traversalSource = parts[0];
        isPrintResult = isPrintResult();

        System.out.printf("Preparing Gremlin traversal string with Source \"%s\":\n\t%s using %s for %s\n",
                            traversalSource,
                            gremlinString,
                            terminator,
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
    public Pair<Boolean,Object> call() throws Exception {

        // If close not performed, there seems to be a leak according to the profiler
        final Traversal.Admin<?,?> resultTraversal = gremlinLangEngine.eval(bytecodeThreadLocal.get(),
                                                                            bindings,
                                                                            traversalSource);
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
        return new Pair<>(true, resultTraversal);
    }

    /*
   Called before the actual workload is executed.
   This is called within the scheduler and is NOT part of the workload measurement.
   In this case we are producing the vertex id, if required, for the Gremlin string.
    */
    @Override
    public void preCall() {
        if (requiresIdFormat) {
            logger.PrintDebug("EvalQueryWorkloadProvider", "Pre Call");
            final Object id = idManager.getId();
            Object[] data = bytecodeThreadLocal.get().getStepInstructions().get(0).getArguments();
            data[0] = id;
            logger.PrintDebug("EvalQueryWorkloadProvider", "Pre Call with id %s", id);
        }
    }

    /*
    Called after the actual workload is executed passing the value type T from the workload.
    This is called within the scheduler and is NOT part of the workload measurement.
     */
    @Override
    public void postCall(Object resultTraversal, Boolean ignored0, Throwable ignored1) {

        if(resultTraversal == null){
            return;
        }

        try {
            logger.PrintDebug("EvalQueryWorkloadProvider", "Post Call");
            ((Traversal.Admin<?,?>)resultTraversal).close();
        } catch (Exception e) {
            logger.Print("EvalQueryWorkloadProvider error during close of the traversal", e);
        }
    }
}
