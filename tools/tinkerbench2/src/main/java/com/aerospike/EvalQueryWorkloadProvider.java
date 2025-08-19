package com.aerospike;

import org.apache.tinkerpop.gremlin.jsr223.GremlinLangScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.JavaTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.javatuples.Pair;

import javax.script.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class EvalQueryWorkloadProvider extends QueryWorkloadProvider {

    final GremlinLangScriptEngine engine;
    final String gremlinString;
    final String traversalSource;
    final LogSource logger;
    final Boolean isPrintResult;
    Terminator terminator = Terminator.toList;
    final String formatDefinedId;
    final boolean formatIdRequired;
    final IdManager idManager;
    final Bindings bindings;
    ThreadLocal<Bytecode> bytecodeThreadLocal;

    final Pattern funcPattern = Pattern.compile("^\\s*(?<stmt>.+)\\.(?<func>[^(]+)\\(\\s*\\)\\s*$", Pattern.CASE_INSENSITIVE);
    ///This is a much more complete regex to parse the Gremlin string. This will allow an advance Id Manager based on String format params...
    final Pattern fmtargPattern = Pattern.compile("(?<arg>(?<begin>['\"][^%]*)?%(?<opts>(?:\\\\d+\\\\$)?(?:[-#+ 0,(<]*)?(?:\\\\d*)?(?:\\\\.\\\\d*)?(?:[tT])?)(?:[a-zA-Z])(?<end>[^)'\"]*['\"])?)");

    enum Terminator {
        none,
        next,
        toList,
        iterate,
        toSet,
        hasNext
    }

    public EvalQueryWorkloadProvider(final WorkloadProvider provider,
                                     final AGSGraphTraversal ags,
                                     String gremlinScript,
                                     final IdManager idManager) {
        super(provider, ags, idManager, gremlinScript);
        logger = getLogger();
        this.idManager = idManager;

        gremlinScript = gremlinScript.replace("'", "\"");

        Matcher matcher = funcPattern.matcher(gremlinScript);
        if (matcher.find()) {
            String terminatorString = matcher.group("func").toLowerCase();
            switch (terminatorString) {
                case "next":
                    terminator = Terminator.next;
                    gremlinScript = matcher.group("stmt");
                    break;
                case "tolist":
                    terminator = Terminator.toList;
                    gremlinScript = matcher.group("stmt");
                    break;
                case "iterate":
                    terminator = Terminator.iterate;
                    gremlinScript = matcher.group("stmt");
                    break;
                case "toset":
                    terminator = Terminator.toSet;
                    gremlinScript = matcher.group("stmt");
                    break;
                case "hasnext":
                    terminator = Terminator.hasNext;
                    gremlinScript = matcher.group("stmt");
                    break;
                default:
                    terminator = Terminator.toList;
                    System.err.println("Defaulting to Terminator Step 'toList'...");
                    logger.Print("EvalQueryWorkloadProvider",false, "Defaulting to Terminator Step 'toList'...");
            }
        }

        final String[] parts = gremlinScript.split("\\.");
        Object sampleId;

        if(this.idManager.isInitialized()) {
            sampleId = this.idManager.getId();
        } else {
            sampleId = null;
        }

        Matcher fmtargMatch = fmtargPattern.matcher(gremlinScript);
        if (fmtargMatch.find()) {
            String fmtArg = fmtargMatch.group("arg");
            if(sampleId instanceof String && !fmtArg.startsWith("\"")) {
                fmtArg = "\"" + fmtArg + "\"";
                sampleId = String.format(fmtArg, sampleId);
                formatIdRequired = true;
            } else if (fmtargMatch.group("opts") != null
                        && !fmtargMatch.group("opts").isEmpty()) {
                sampleId = String.format(fmtArg, sampleId);
                formatIdRequired = true;
            } else {
                formatIdRequired = false;
            }

            formatDefinedId = fmtArg;
        }
        else {
            formatDefinedId = null;
            formatIdRequired = false;
            sampleId = null;
        }

        this.gremlinString = gremlinScript;
        this.traversalSource = parts[0];
        isPrintResult = isPrintResult();

        System.out.printf("Preparing Gremlin traversal string with Source \"%s\":\n\t%s using %s for %s\n",
                            traversalSource,
                            gremlinString,
                            terminator,
                            isWarmup() ? "Warmup" : "Workload");

        logger.PrintDebug("EvalQueryWorkloadProvider", "Getting GremlinLangScriptEngine Engine");
        engine = new GremlinLangScriptEngine();

        logger.PrintDebug("EvalQueryWorkloadProvider", "Binding to g");
        bindings = engine.createBindings();
        bindings.put(traversalSource, G());

        try {
            logger.PrintDebug("EvalQueryWorkloadProvider", "Generating Bytecode for \"%s\"", gremlinString);

            if(formatDefinedId != null && !this.idManager.isInitialized()) {
                throw new ScriptException(String.format("Script contains a 'id' placeholder but the Id Manager was not initialized (disabled). Id Manager is required!\n'%s'",
                                                        gremlinString));
            }

            final Object finalSampleId = sampleId;
            bytecodeThreadLocal = ThreadLocal.withInitial(() -> {
                try {
                    return ((DefaultGraphTraversal<?, ?>)
                                engine.eval(String.format(gremlinString, finalSampleId),
                                            bindings))
                                            .getBytecode();
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
                } catch (Exception e) {
                    System.err.printf("ERROR: could not evaluate gremlin script \"%s\". Error: %s\n",
                            gremlinString,
                            e.getMessage());
                    logger.error(String.format("ERROR: could not evaluate gremlin script \"%s\". Error: %s\n",
                                    gremlinString,
                                    e.getMessage()),
                            e);
                    getOpenTelemetry().addException(e);
                    throw new RuntimeException(e);
                }
                return null;
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

    public String BytecodeTranslator() {
        JavaTranslator<GraphTraversalSource, Traversal.Admin<?, ?>> translator = JavaTranslator.of(G());
        try(Traversal<?, ?> translatedTraversal = translator.translate(bytecodeThreadLocal.get())) {
            return translatedTraversal.toString();
        } catch (Exception e) {
            logger.PrintDebug("EvalQueryWorkloadProvider", e);
        }
        return null;
    }

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
    public int getSampleSize() {
        return formatDefinedId == null ? 0 : -1;
    };

    @Override
    public Pair<Boolean,Object> call() throws Exception {

        // If close not performed, there seems to be a leak according to the profiler
        final Traversal.Admin<?,?> resultTraversal = engine.eval(bytecodeThreadLocal.get(),
                                                                            bindings,
                                                                            traversalSource);
        if (isPrintResult) {
            resultTraversal.forEachRemaining(this::PrintResult);
        } else {
            switch (terminator) {
                case next:
                    resultTraversal.next();
                    break;
                case hasNext:
                    resultTraversal.hasNext();
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
                    logger.Print("EvalQueryWorkloadProvider", true, "Unknown terminator: '%s'", terminator);
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

        if (formatDefinedId != null) {
            logger.PrintDebug("EvalQueryWorkloadProvider", "Pre Call");
            final Object id = idManager.getId();
            Object[] data = bytecodeThreadLocal.get().getStepInstructions().get(0).getArguments();
            data[0] = formatIdRequired
                        ? String.format(formatDefinedId,id)
                        : id;
            if(logger.isDebug()) {
                logger.PrintDebug("EvalQueryWorkloadProvider", "Pre Call with id %s", id);
                logger.PrintDebug("EvalQueryWorkloadProvider", BytecodeTranslator());
            }
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
