package com.aerospike;

import org.apache.tinkerpop.gremlin.jsr223.GremlinLangScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.JavaTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.javatuples.Pair;

import javax.script.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class EvalQueryWorkloadProvider extends QueryWorkloadProvider {

    final String gremlinString;
    final LogSource logger;
    final Boolean isPrintResult;
    final IdManager idManager;
    final Matcher fmtargMatch;

    GremlinLangScriptEngine engine;
    String traversalSource;
    Terminator terminator = Terminator.toList;
    //The string format argument required to format the id...
    String formatDefinedId;
    //Can just use the id directory, no string format required...
    boolean formatIdRequired;
    Bindings bindings;
    ThreadLocal<Bytecode> bytecodeThreadLocal;
    boolean prepared = false;
    final AtomicBoolean compiled = new AtomicBoolean(false);

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

        this.gremlinString = gremlinScript;
        isPrintResult = isPrintResult();

        fmtargMatch = fmtargPattern.matcher(gremlinString);
        if (fmtargMatch.find()) {
            formatDefinedId = fmtargMatch.group("arg");
            formatIdRequired = true;
        } else {
            formatDefinedId = null;
            formatIdRequired = false;
        }
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
    public void PrepareCompile() {

        logger.PrintDebug("PrepareCompile",
                            "Starting...");

        if(prepared) {
            logger.PrintDebug("PrepareCompile",
                            "Already Prepared... Skipping...");
            return;
        }

        if(getProvider().isAborted()) {
            logger.PrintDebug("PrepareCompile",
                    "Abort Signaled... Aborting...");
            return;
        }

        prepared = true;
        final String[] parts = gremlinString.split("\\.");
        Object sampleId = null;

        if (formatDefinedId != null && this.idManager.isInitialized()) {
            sampleId = this.idManager.getId();
            if(sampleId instanceof String && !formatDefinedId.startsWith("\"")) {
                formatDefinedId = "\"" + formatDefinedId + "\"";
                sampleId = String.format(formatDefinedId, sampleId);
            } else if (fmtargMatch.group("opts") != null
                    && !fmtargMatch.group("opts").isEmpty()) {
                sampleId = String.format(formatDefinedId, sampleId);
            } else {
                //No need to format the id (can use it directly)
                formatIdRequired = false;
            }
        }

        this.traversalSource = parts[0];

        System.out.printf("Preparing Gremlin traversal string with Source \"%s\":\n\t%s using %s for %s\n",
                            traversalSource,
                            gremlinString,
                            terminator,
                            isWarmup() ? "Warmup" : "Workload");

        logger.PrintDebug("PrepareCompile", "Getting GremlinLangScriptEngine Engine");
        engine = new GremlinLangScriptEngine();

        logger.PrintDebug("PrepareCompile", "Binding to g");
        bindings = engine.createBindings();
        bindings.put(traversalSource, G());

        try {
            logger.PrintDebug("PrepareCompile", "Generating Bytecode for \"%s\"", gremlinString);

            if(formatDefinedId != null && sampleId == null) {
                throw new ScriptException(String.format("Script contains a 'id' placeholder but the Id Manager was not initialized (disabled). Id Manager is required!\n'%s'",
                                                        gremlinString));
            }

            final Object finalSampleId = sampleId;
            bytecodeThreadLocal = ThreadLocal.withInitial(() -> {
                try {
                    final Bytecode result = ((DefaultGraphTraversal<?, ?>)
                                                engine.eval(String.format(gremlinString, finalSampleId),
                                                        bindings))
                                                .getBytecode();
                    compiled.set(true);
                    return result;
                /*} catch (ScriptException e) {
                    System.err.printf("ERROR: could not evaluate gremlin script \"%s\". Error: %s\n",
                            gremlinString,
                            e.getMessage());
                    logger.error(String.format("ERROR: could not evaluate gremlin script \"%s\". Error: %s\n",
                                    gremlinString,
                                    e.getMessage()),
                            e);
                    getOpenTelemetry().addException(e);
                    getProvider().SignalAbortWorkLoad();*/
                } catch (Exception e) {
                    System.err.printf("ERROR: could not evaluate gremlin script \"%s\". Error: %s\n",
                            gremlinString,
                            e.getMessage());
                    logger.error(String.format("ERROR: could not evaluate gremlin script \"%s\". Error: %s\n",
                                    gremlinString,
                                    e.getMessage()),
                            e);
                    getProvider().AddError(e);
                    getProvider().SignalAbortWorkLoad();
                }
                return null;
            });

            logger.PrintDebug("PrepareCompile",
                    "Executing 'precall'");
            
            this.preCall();
            logger.PrintDebug("PrepareCompile",
                    "Executing 'call'");
            this.call();

        } catch (Exception e) {
            System.err.printf("ERROR: could not evaluate gremlin script \"%s\". Error: %s\n",
                    gremlinString,
                    e.getMessage());
            logger.error(String.format("ERROR: could not evaluate gremlin script \"%s\". Error: %s\n",
                            gremlinString,
                            e.getMessage()),
                    e);
            getProvider().AddError(e);
            getProvider().SignalAbortWorkLoad();
        }

        logger.PrintDebug("PrepareCompile",
                "%s...",
                getProvider().isAborted()
                        ? "Aborted"
                        : "Completed");
    }

    public String BytecodeTranslator() {
        final Bytecode bytecode = bytecodeThreadLocal.get();
        if(bytecode != null) {
            JavaTranslator<GraphTraversalSource, Traversal.Admin<?, ?>> translator = JavaTranslator.of(G());
            try (Traversal<?, ?> translatedTraversal = translator.translate(bytecode)) {
                return translatedTraversal.toString();
            } catch (Exception e) {
                logger.PrintDebug("EvalQueryWorkloadProvider", e);
            }
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

        if(!getProvider().isAborted()) {
            final Bytecode bytecode = bytecodeThreadLocal.get();

            if (formatDefinedId != null && bytecode != null) {
                logger.PrintDebug("EvalQueryWorkloadProvider.preCall", "Pre Call");
                final Object id = idManager.getId();
                final Object[] data = bytecode.getStepInstructions().get(0).getArguments();
                if (formatIdRequired) {
                    data[0] = String.format(formatDefinedId, id);
                } else {
                    data[0] = id;
                }
                if (logger.isDebug()) {
                    logger.PrintDebug("EvalQueryWorkloadProvider.preCall", "Pre Call with id %s", id);
                    logger.PrintDebug("EvalQueryWorkloadProvider.preCall", BytecodeTranslator());
                }
            } else if (bytecode == null) {
                logger.PrintDebug("EvalQueryWorkloadProvider.preCall", "Bytecode is null");
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

    public String ToString() {
        return String.format("{\"query\":\"%s\", \"Prepared\":%s, \"Compiled\":%s, \"formatDefinedId\":\"%s\"}",
                                this.gremlinString,
                                this.prepared,
                                this.compiled.get(),
                                this.formatDefinedId);
    }
}
