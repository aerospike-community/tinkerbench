package com.aerospike;

import org.apache.tinkerpop.gremlin.jsr223.GremlinLangScriptEngine;
import org.apache.tinkerpop.gremlin.jsr223.JavaTranslator;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.DefaultGraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.translator.GroovyTranslator;
import org.javatuples.Pair;

import javax.script.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class EvalQueryWorkloadProvider extends QueryWorkloadProvider {

    final String gremlinString;
    final LogSource logger;
    final Boolean isPrintResult;
    final IdManager idManager;
    final Terminator terminator;

    ///  Used to manage the gremlin string placeholders
    final FmtArgInfo idFmtArgsPos;
    final AtomicBoolean compiled = new AtomicBoolean(false);

    final static Pattern funcPattern = Pattern.compile("^\\s*(?<stmt>.+)\\.(?<func>[^(]+)\\(\\s*\\)\\s*$", Pattern.CASE_INSENSITIVE);

    GremlinLangScriptEngine engine;
    String traversalSource;

    ThreadLocal<Helpers.MutablePair<Bytecode, Bindings>> bytecodeThreadLocal;
    boolean prepared = false;

    public enum Terminator {
        none,
        next,
        toList,
        iterate,
        toSet,
        hasNext
    }

    public static Pair<String,Terminator> DetermineScriptTerminator(final String gremlinScript) {
        final Matcher matcher = funcPattern.matcher(gremlinScript);

        if (matcher.find()) {
            final String terminatorString = matcher.group("func").toLowerCase();
            return switch (terminatorString) {
                case "next" -> Pair.with(matcher.group("stmt"), Terminator.next);
                case "tolist" -> Pair.with(matcher.group("stmt"), Terminator.toList);
                case "iterate" -> Pair.with(matcher.group("stmt"), Terminator.iterate);
                case "toset" -> Pair.with(matcher.group("stmt"), Terminator.toSet);
                case "hasnext" -> Pair.with(matcher.group("stmt"), Terminator.hasNext);
                default -> Pair.with(gremlinScript, Terminator.none);
            };
        }
        return Pair.with(gremlinScript,Terminator.none);
    }

    public static Terminator DetermineTerminator(final String gremlinScript) {
        final Matcher matcher = funcPattern.matcher(gremlinScript);

        if (matcher.find()) {
            String terminatorString = matcher.group("func").toLowerCase();
            return switch (terminatorString) {
                case "next" -> Terminator.next;
                case "tolist" -> Terminator.toList;
                case "iterate" -> Terminator.iterate;
                case "toset" -> Terminator.toSet;
                case "hasnext" -> Terminator.hasNext;
                default -> Terminator.none;
            };
        }
        return Terminator.none;
    }

    public EvalQueryWorkloadProvider(final WorkloadProvider provider,
                                     final AGSGraphTraversal ags,
                                     final String gremlinScript,
                                     final IdManager idManager) {
        super(provider, ags, idManager, gremlinScript);
        logger = getLogger();
        this.idManager = idManager;

        final Pair<String,Terminator> gremlinStep = DetermineScriptTerminator(gremlinScript.replace("'", "\""));
        this.gremlinString = gremlinStep.getValue0();
        if(gremlinStep.getValue1() == Terminator.none) {
            System.err.println("Defaulting Gremlin Query Terminator Step 'toList'...");
            logger.warn("Defaulting Gremlin Query Terminator Step 'toList'...");
            this.terminator = Terminator.toList;
        } else {
            this.terminator = gremlinStep.getValue1();
        }
        isPrintResult = isPrintResult();
        this.idFmtArgsPos = new FmtArgInfo(this.gremlinString, this.idManager);
    }

    /*private static void GetEngines(ScriptEngineManager manager) {
        for (ScriptEngineFactory factory : manager.getEngineFactories()) {
            System.out.println("Engine Name: " + factory.getEngineName());
            System.out.println("Language Name: " + factory.getLanguageName());
            System.out.println("Names: " + factory.getNames());
            System.out.println("--------------------");
        }
    }*/

    private Bindings createBindings() {

        final Bindings bindings = engine.createBindings();
        bindings.put(traversalSource, G());

        if (this.idFmtArgsPos.length() > 0) {
            logger.PrintDebug("EvalQueryWorkloadProvider.getBindings", "Pre Call");
            final Object[] useIds = this.idFmtArgsPos.getIds();

            for (FmtArgInfo.FmtArg fmtId : this.idFmtArgsPos.args()) {
                bindings.put(fmtId.phVarName, useIds[fmtId.position - 1]);
            }

            logger.PrintDebug("EvalQueryWorkloadProvider.getBindings", "Pre Call with id %s", useIds);
        }

        logger.PrintDebug("EvalQueryWorkloadProvider.getBindings", bindings.toString());

        return  bindings;
    }

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
        this.idFmtArgsPos.init();

        final String gremlinString = this.idFmtArgsPos.gremlinString();

        this.traversalSource = parts[0];

        System.out.printf("Preparing Gremlin traversal string with Source \"%s\":\n\t%s using %s for %s\n",
                            traversalSource,
                            gremlinString,
                            terminator,
                            isWarmup() ? "Warmup" : "Workload");

        logger.PrintDebug("PrepareCompile", "Getting GremlinLangScriptEngine Engine");
        engine = new GremlinLangScriptEngine();

        logger.PrintDebug("PrepareCompile", "Binding to " + this.traversalSource);

        try {
            logger.PrintDebug("PrepareCompile", "Generating Bytecode for \"%s\"", gremlinString);

            final Object[] sampleIds = this.idFmtArgsPos.getIds(this.idFmtArgsPos.maxArgsPosition);

            logger.PrintDebug("PrepareCompile", "Gremlin String Ids: %s", sampleIds);

            if(sampleIds.length == 0 && this.idFmtArgsPos.length() > 0) {
                throw new ScriptException(String.format("Script contains 'id' placeholders but the Id Manager was not initialized (disabled?). Id Manager is required!\n'%s'",
                                                        gremlinString));
            }
            if(sampleIds.length > (this.idManager.getInitialDepth() + 1)) {
                throw new ScriptException(String.format("Script contains Chaining/Depth 'id' placeholders requiring a maximum dept of %d, but The Id Manager can only provide a depth of %d. Are you using the Correct Id Manager, Not providing/importing enough ids, or Gremlin string is incorrect?\n'%s'",
                                                        this.idFmtArgsPos.maxArgs(),
                                                        (this.idManager.getInitialDepth() + 1),
                                                        gremlinString));
            }

            final String sampleQuery = String.format("Sample Gremlin Query: '%s'",
                                                        String.format(this.idFmtArgsPos.fmtArgString,
                                                                        sampleIds));
            logger.info(sampleQuery);
            Helpers.Println(System.out,
                            sampleQuery,
                            Helpers.BLACK,
                            Helpers.GREEN_BACKGROUND);

            bytecodeThreadLocal = ThreadLocal.withInitial(() -> {
                final Bindings bindings = this.createBindings();
                try {
                    final Bytecode result = ((DefaultGraphTraversal<?, ?>)
                                                engine.eval(this.idFmtArgsPos.gremlinString(),
                                                            bindings))
                                                .getBytecode();
                    compiled.set(true);
                    return new Helpers.MutablePair<>(result, bindings);
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
                    System.err.printf("ERROR: could not evaluate gremlin script \"%s\" (check gremlin syntax?). Error: %s\n",
                                        gremlinString,
                                        e.getMessage());
                    logger.error(String.format("ERROR: could not evaluate gremlin script \"%s\". Error: %s\n",
                                                gremlinString,
                                                e.getMessage()),
                            e);
                    getProvider().AddError(e);
                    getProvider().SignalAbortWorkLoad();
                }
                return new Helpers.MutablePair<>(null, bindings);
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
        final Helpers.MutablePair<Bytecode,Bindings> bytecodePair = bytecodeThreadLocal.get();
        if(bytecodePair.first != null) {
            JavaTranslator<GraphTraversalSource, Traversal.Admin<?, ?>> translator = JavaTranslator.of(G());
            try (Traversal<?, ?> translatedTraversal = translator.translate(bytecodePair.first)) {
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
        return this.idFmtArgsPos.length() == 0 ? 0 : -1;
    }

    public void PrintResult(Traversal.Admin<?,?> resultTraversal) {
        String gremlinQuery = GroovyTranslator
                                .of(traversalSource)
                                .translate(resultTraversal.getBytecode())
                                .getScript();
        Object result = resultTraversal.toList();

        try {
            System.out.print(Helpers.GREEN_BACKGROUND);
            System.out.print(Helpers.BLACK);

            System.out.println();
            System.out.println(gremlinQuery);
            System.out.print(Helpers.BLACK_BACKGROUND);
            System.out.print(Helpers.YELLOW);
            switch (result) {
                case ArrayList<?> arrayLst -> System.out.println(Arrays.toString(arrayLst.toArray()));
                case HashMap<?, ?> map ->
                        map.forEach((key, value) -> System.out.println("Key: " + key + ", Value: " + value));
                case HashSet<?> hashSet -> hashSet.forEach(System.out::println);
                default -> System.out.println(result);
            }
        }
        finally {
            System.out.println(Helpers.RESET);
        }
    }

    @Override
    public Pair<Boolean,Object> call() throws Exception {

        final Helpers.MutablePair<Bytecode,Bindings> bytecodePair = bytecodeThreadLocal.get();

        // If close not performed, there seems to be a leak according to the profiler
        final Traversal.Admin<?,?> resultTraversal = engine.eval(bytecodePair.first,
                                                                    bytecodePair.second,
                                                                    traversalSource);
        if (isPrintResult) {
            this.PrintResult(resultTraversal);
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
            final Helpers.MutablePair<Bytecode,Bindings> bytecodePair = bytecodeThreadLocal.get();
            Bindings bindings = createBindings();

            if (bytecodePair.first != null) {
                logger.PrintDebug("EvalQueryWorkloadProvider.preCall", "Pre Call");
                bytecodePair.second = bindings;
            } else {
                logger.PrintDebug("EvalQueryWorkloadProvider.preCall", "Bytecode was not produced (null). Gremlin syntax error?");
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

    @Override
    public String toString() {
        return String.format("{\"query\":\"%s\", \"Prepared\":%s, \"Compiled\":%s, \"IdsPlaceHolders\":%d \"Depth\":%d}",
                                this.gremlinString,
                                this.prepared,
                                this.compiled.get(),
                                this.idFmtArgsPos.length(),
                                this.idFmtArgsPos.maxArgs());
    }
}
