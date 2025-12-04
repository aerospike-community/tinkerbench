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

    ///  Array of Id Format Id Args and the Max Depth
    final Pair<FmtArgInfo[], Integer> idFmtArgsDepth;
    final AtomicBoolean compiled = new AtomicBoolean(false);

    final Pattern funcPattern = Pattern.compile("^\\s*(?<stmt>.+)\\.(?<func>[^(]+)\\(\\s*\\)\\s*$", Pattern.CASE_INSENSITIVE);

    GremlinLangScriptEngine engine;
    String traversalSource;
    Terminator terminator = Terminator.toList;

    Bindings bindings;
    ThreadLocal<Bytecode> bytecodeThreadLocal;
    boolean prepared = false;

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

        final Matcher matcher = funcPattern.matcher(gremlinScript);
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
        this.idFmtArgsDepth = FmtArgInfo.determineFmtArgs(gremlinScript);
        this.idManager.setDepth(this.idFmtArgsDepth.getValue1());
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

        final String gremlinString = FmtArgInfo.determineGremlinString(this.idFmtArgsDepth,
                                                                        this.idManager,
                                                                        this.gremlinString);

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

            final Object[] sampleIds = FmtArgInfo.getIds(this.idFmtArgsDepth, this.idManager);

            logger.PrintDebug("PrepareCompile", "Gremlin String Ids: %s", sampleIds);

            if(sampleIds.length == 0 && this.idFmtArgsDepth.getValue0().length > 0) {
                throw new ScriptException(String.format("Script contains an 'id' placeholder but the Id Manager was not initialized (disabled). Id Manager is required!\n'%s'",
                                                        gremlinString));
            }
            if(sampleIds.length > this.idManager.getDepth()) {
                throw new ScriptException(String.format("Script contains Chaining/Depth 'id' placeholders (max. depth of %d) but The Id Manager can only provide a depth of %d. Are you using the Correct Id Manager or Not providing enough id depth?\n'%s'",
                        this.idFmtArgsDepth.getValue1(),
                        this.idManager.getDepth(),
                        gremlinString));
            }

            bytecodeThreadLocal = ThreadLocal.withInitial(() -> {
                try {
                    final Bytecode result = ((DefaultGraphTraversal<?, ?>)
                                                engine.eval(String.format(gremlinString, sampleIds),
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
        return this.idFmtArgsDepth.getValue0().length == 0 ? 0 : -1;
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

            if (this.idFmtArgsDepth.getValue0().length > 0 && bytecode != null) {
                logger.PrintDebug("EvalQueryWorkloadProvider.preCall", "Pre Call");
                final Object[] ids = FmtArgInfo.getIds(this.idFmtArgsDepth, this.idManager);
                final Object[] data = bytecode.getStepInstructions().get(0).getArguments();
                data[0] = ids;
                if (logger.isDebug()) {
                    logger.PrintDebug("EvalQueryWorkloadProvider.preCall", "Pre Call with id %s", ids);
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
        return String.format("{\"query\":\"%s\", \"Prepared\":%s, \"Compiled\":%s, \"IdsPlaceHolders\":%d \"Depth\":%d}",
                                this.gremlinString,
                                this.prepared,
                                this.compiled.get(),
                                this.idFmtArgsDepth.getValue0().length,
                                this.idFmtArgsDepth.getValue1());
    }
}
