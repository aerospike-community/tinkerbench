package com.aerospike;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.*;
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.resources.Resource;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.opentelemetry.semconv.ServiceAttributes.SERVICE_NAME;

public final class OpenTelemetryExporter implements com.aerospike.OpenTelemetry {

    private final String SCOPE_NAME = "com.aerospike.workload";
    private final String METRIC_NAME = "aerospike.workload.ags";

    private final int prometheusPort;
    private String workloadName;
    private String connectionState;
    private String wlTypeStage = "";
    private final long pid = ProcessHandle.current().pid();
    private final int closeWaitMS;

    private final Attributes[] hbAttributes;
    private final OpenTelemetrySdk openTelemetrySdk;
    private final LongGauge openTelemetryInfoGauge;
    private final LongGauge openTelemetryIdMgrGauge;
    private final LongCounter openTelemetryExceptionCounter;
    private final LongUpDownCounter openTelemetryPendingCounter;
    private final DoubleHistogram openTelemetryLatencyMSHistogram;

    private int isWarmup = 0; //0 -- unknown, 1 -- Warmup, 2 -- Workload
    private String workloadType;
    private final AtomicInteger hbCnt = new AtomicInteger();
    private final long startTimeMillis;
    private final LocalDateTime startLocalDateTime;
    private long endTimeSecs;
    private LocalDateTime endLocalDateTime;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final LogSource logger = LogSource.getInstance();

    public final AtomicBoolean abortRun;
    public final AtomicBoolean terminateRun;

    public OpenTelemetryExporter(TinkerBenchArgs args,
                                 StringBuilder otherInfo) {

        Helpers.Println(System.out,
                        "Prometheus Enabled",
                        Helpers.BLACK,
                        Helpers.GREEN_BACKGROUND);
        logger.info("Prometheus Enabled");

        this.startTimeMillis = System.currentTimeMillis();
        this.startLocalDateTime = LocalDateTime.ofInstant(Instant.ofEpochMilli(this.startTimeMillis),
                                    ZoneId.systemDefault());
        this.prometheusPort = args.promPort;
        this.connectionState = "Initializing";
        this.closeWaitMS =(int) args.closeWaitDuration.toMillis();
        this.abortRun = args.abortSIGRun;
        this.terminateRun = args.terminateRun;

        this.printDebug("Creating OpenTelemetryExporter");

        this.openTelemetrySdk = this.initOpenTelemetry();
        Meter openTelemetryMeter = this.openTelemetrySdk.getMeter(SCOPE_NAME);

        this.printDebug("Creating Metrics");

        this.openTelemetryInfoGauge =
                openTelemetryMeter
                        .gaugeBuilder(METRIC_NAME + ".stateinfo")
                        .setDescription("Aerospike Workload Config/State Information")
                        .ofLongs()
                        .build();

        this.openTelemetryIdMgrGauge =
                openTelemetryMeter
                        .gaugeBuilder(METRIC_NAME + ".idmgrinfo")
                        .setDescription("Aerospike Workload Id Manager Information")
                        .ofLongs()
                        .build();

        this.openTelemetryExceptionCounter =
                openTelemetryMeter
                    .counterBuilder(METRIC_NAME + ".exception")
                        .setDescription("Aerospike Workload Exception")
                        .build();

        this.openTelemetryPendingCounter =
                openTelemetryMeter
                        .upDownCounterBuilder(METRIC_NAME + ".pending.run.queue")
                        .setDescription("The number of pending Gremlin actions.")
                        .build();

        this.openTelemetryLatencyMSHistogram =
                openTelemetryMeter
                        .histogramBuilder(METRIC_NAME + ".lng.latency")
                        .setDescription("Aerospike Workload Latencies (ms)")
                        .setUnit("ms")
                        .build();

        this.printDebug("SDK and Metrics Completed");

        this.hbAttributes = new Attributes[4];

        this.printDebug("Creating Signal Handler...");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            if(!this.closed.get()) {
                System.out.println("OTel Shutdown hook. Performing cleanup...");
                this.setConnectionStateAbort("Abort Requested");
            }
        }));

        try {
            Thread.sleep(500);
        } catch (InterruptedException ignored) {}
    }

    /*
    private void Initialize(String wlStage,
                                long currentTimeMS) {

        this.printDebug(String.format("Initialize '%s' at %d",
                                        wlStage, currentTimeMS));

        AttributesBuilder attrBuilder = Attributes.builder();

        attrBuilder.putAll( Attributes.of(
                                AttributeKey.stringKey("workload"), "NA",
                                AttributeKey.stringKey("wlstage"), wlStage,
                                AttributeKey.stringKey("wltype"), "NA",
                                AttributeKey.longKey("pid"), this.pid,
                                AttributeKey.longKey("startTimeSecs"), Math.round(this.startTimeMillis / 1000.0)
            ));

        final Attributes keyAttrs = attrBuilder.build();

        this.openTelemetryPendingCounter.add(0, keyAttrs);
        this.openTelemetryExceptionCounter.add(0, keyAttrs);
        this.openTelemetryLatencyMSHistogram.record((double) 0, keyAttrs);

        attrBuilder.put("type", "static");
        attrBuilder.putAll(
                Attributes.of(
                        AttributeKey.stringKey("otherInfo"), null,
                        AttributeKey.stringKey("startDateTime"), null,
                        AttributeKey.stringKey("AGSHost"), null
                ));

        if(this.connectionState != null) {
            attrBuilder.put("DBConnState", this.connectionState);
        }

        attrBuilder.put("currTimeSecs", currentTimeMS / 1000L);

        this.openTelemetryInfoGauge.set(currentTimeMS * 100L,
                                        attrBuilder.build());

        attrBuilder = Attributes.builder();
        attrBuilder.putAll(keyAttrs);

        attrBuilder.putAll(Attributes.of(
                            AttributeKey.stringKey("type"), "static",
                            AttributeKey.stringKey("manger_class"), "NA",
                            AttributeKey.stringKey("label"), "NA",
                            AttributeKey.longKey("requested_cnt"), (long) -1,
                            AttributeKey.longKey("actual_cnt"), (long) 0,
                            AttributeKey.longKey("runtimems"), (long) 0
        ));

        attrBuilder.putAll(Attributes.of(
                AttributeKey.stringKey("gremlinIdString"),  "NA"
        ));

        openTelemetryIdMgrGauge.set(currentTimeMS,
                                    attrBuilder.build());

        this.printDebug(String.format("Initialized '%s' at %d",
                                        wlStage, currentTimeMS));
    }

    @Override
    public void Initialize() {

        if(this.closed.get()) { return; }
        final long now = System.currentTimeMillis();

        hbCnt.set(0);

        for(int i  = 0; i < this.hbCnt.get(); i++) {
            this.hbAttributes[i] = null;
        }
        Initialize("Warmup", now);
        Initialize("Workload", now);
    }
    */

    private OpenTelemetrySdk initOpenTelemetry() {
        // Include required service.name resource attribute on all spans and metrics
        this.printDebug("Creating SDK");

        Resource resource =
                Resource.getDefault()
                        .merge(Resource
                                .builder()
                                .put(SERVICE_NAME, "PrometheusExporterAerospikeWL")
                                .build());

        return OpenTelemetrySdk.builder()
                /*.setTracerProvider(
                        SdkTracerProvider.builder()
                                .setResource(resource)
                                .addSpanProcessor(SimpleSpanProcessor.create(LoggingSpanExporter.create()))
                                .build())*/
                .setMeterProvider(
                        SdkMeterProvider.builder()
                                .setResource(resource)
                                .registerMetricReader(
                                        PrometheusHttpServer.builder().setPort(prometheusPort).build())
                                .build())
                .buildAndRegisterGlobal();
    }

    @Override
    public boolean isEnabled() { return true; }

    private void printDebug(String msg, boolean limited) {
        logger.PrintDebug("OPENTEL", msg, limited);
    }
    private void printDebug(String msg) {
        logger.PrintDebug("OPENTEL", msg);
    }
    private void printMsg(String msg, boolean includeTimestamp) {

        if(includeTimestamp) {
            LocalDateTime now = LocalDateTime.now();
            String formattedDateTime = now.format(LogSource.DateFormatter);

            System.out.printf("%s %s%n", formattedDateTime, msg);
        }
        else {
            System.out.printf("%s%n", msg);
        }
    }

    private void updateInfoGauge(boolean initial) {
        this.updateInfoGauge(initial, false);
    }

    private void updateInfoGauge(boolean initial, boolean force) {

        if(!force && this.closed.get()) { return; }

        final AttributesBuilder attributes = Attributes.builder();

        attributes.put("type", "static");
        for (Attributes attrItem : this.hbAttributes) {
            attributes.putAll(attrItem);
        }

        if(this.connectionState != null) {
            attributes.put("DBConnState", this.connectionState);
        }
        if(this.endTimeSecs != 0) {
            attributes.put("endTimeSecs", this.endTimeSecs);
            attributes.put("endLocalDateTime", this.endLocalDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            attributes.put("runDurationSecs", Math.round(this.endTimeSecs - (this.startTimeMillis / 1000.0)));
        }

        long hCnt = (long) hbCnt.incrementAndGet();
        if(hCnt >= 90) {
            hCnt = 0;
            hbCnt.set(0);
        }
        final long now = System.currentTimeMillis();
        final long counter = (now * 100L)
                                + hCnt ;

        attributes.put("currTimeSecs", now / 1000L);

        this.openTelemetryInfoGauge.set(counter,
                                        attributes.build());

        logger.PrintDebug("OpenTelemetry", "Info Gauge %d", counter);
    }

    @Override
    public void setIdMgrGauge(final String mgrClass,
                                final String[] labels,
                                final String gremlinString,
                                final int requestedCnt,
                                final int actualCnt,
                                final long runtimeMills) {
        if(this.closed.get()) { return; }

        final AttributesBuilder attributes = Attributes.builder();
        attributes.putAll(this.hbAttributes[0]);
        attributes.put("wlstage", "idMgr");

        String label = "NA";

        if(labels != null && labels.length > 0) {
            if(labels.length == 1) {
                label = labels[0];
            } else {
                label = Arrays.toString(labels);
            }
        }

        attributes.putAll(Attributes.of(
                AttributeKey.stringKey("type"), "static",
                AttributeKey.stringKey("manger_class"), mgrClass == null ? "NA" : mgrClass,
                AttributeKey.stringKey("label"), label == null ? "NA" : label,
                AttributeKey.longKey("requested_cnt"), (long) requestedCnt,
                AttributeKey.longKey("actual_cnt"), (long) actualCnt,
                AttributeKey.longKey("runtimems"), runtimeMills
        ));

        attributes.putAll(Attributes.of(
                AttributeKey.stringKey("gremlinIdString"), gremlinString == null ? "NA" : gremlinString
        ));

        openTelemetryIdMgrGauge.set(System.currentTimeMillis(),
                                    attributes.build());
    }

    public void Reset(TinkerBenchArgs args,
                      String workloadName,
                      String workloadType,
                      Duration targetDuration,
                      long pendingActions,
                      boolean warmup,
                      StringBuilder otherInfo) {

        this.isWarmup = warmup ? 1 : 2;
        this.workloadType = workloadType == null ? "Initial" : workloadType;
        this.wlTypeStage = String.format("%s-%s",
                                            warmup ? "Warmup" : "Workload",
                                            this.workloadType);
        this.workloadName = workloadName == null ? "NA" : workloadName;

        this.endTimeSecs = 0;
        this.endLocalDateTime = null;

        if(this.hbAttributes[0] != null
                && workloadType != null
                && pendingActions > 0)
            this.pendingTransCounter(pendingActions * -1);

        //These attributes commonly used by all events
        // They uniquely  identify events to this run
        this.hbAttributes[0] =
                Attributes.of(
                        AttributeKey.stringKey("workload"), this.workloadName,
                        AttributeKey.stringKey("wlstage"), warmup ? "Warmup" : "Workload",
                        AttributeKey.stringKey("wltype"), this.workloadType,
                        AttributeKey.longKey("pid"), this.pid,
                        AttributeKey.longKey("startTimeSecs"), Math.round(this.startTimeMillis / 1000.0)
                );
        //These attributes should be considered static
        this.hbAttributes[1] =
                Attributes.of(
                        AttributeKey.stringKey("otherInfo"), otherInfo == null ? null : otherInfo.toString(),
                        AttributeKey.stringKey("startDateTime"), this.startLocalDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                        AttributeKey.stringKey("AGSHost"), args.agsHosts[0]
                );
        this.hbAttributes[2] =
                Attributes.of(
                        AttributeKey.stringKey("commandlineargs"), String.join(" ", args.getArguments(true)),
                        AttributeKey.stringKey("appversion"), args.getVersions(true)[0]
                );
        this.hbAttributes[3] =
                Attributes.of(
                        AttributeKey.longKey("CallsPerSecond"), (long) args.queriesPerSecond,
                        AttributeKey.longKey("schedulars"), (long) args.schedulers,
                        AttributeKey.longKey("workers"), (long) args.workers,
                        AttributeKey.stringKey("duration"), targetDuration.toString(),
                        AttributeKey.longKey("durationSecs"), targetDuration.toSeconds(),
                        AttributeKey.longKey("errorlimit"), (long) args.errorsAbort
                );

        this.updateInfoGauge(true);
        if(workloadType != null) {
            this.pendingTransCounter(0);
        }
    }

    @Override
    public void addException(final Exception exception) {
        if(this.closed.get()) { return; }

        String errMessage = exception.getMessage();
        if(exception instanceof ResponseException) {
            ResponseException re = (ResponseException) exception;
            StringBuilder sb = errMessage == null
                                    ? new StringBuilder()
                                    : new StringBuilder(errMessage);
            sb.append(" Response Code: ");
            sb.append(re.getResponseStatusCode());
            Optional<List<String>> items = re.getRemoteExceptionHierarchy();
            if(items.isPresent()) {
                sb.append(": Remote");
                for(String item : items.get()) {
                    sb.append(": ");
                    sb.append(item);
                }
            }
            errMessage = sb.toString();
        }

        final String exceptionType = Helpers.GetShortClassName(exception.getClass().getName());
        final String message = Helpers.GetShortErrorMsg(errMessage, 160);

        this.addException(exceptionType, message);
    }

    @Override
    public void addException(final String exceptionType, String message) {

        if(this.closed.get()) { return; }

        //Ignore Cluster Closed exceptions when the app is terminating
        if((terminateRun.get() || abortRun.get()) && Objects.equals(message, "Cluster has been closed")) {
            return;
        }

        final AttributesBuilder attributes = Attributes.builder();

        if(message == null || message.isEmpty()) {
            message = "<No Defined Msg>";
        }

        attributes.putAll(this.hbAttributes[0]);
        attributes.putAll(Attributes.of(
                AttributeKey.stringKey("exception_type"), exceptionType,
                AttributeKey.stringKey("exception"), message
        ));

        this.openTelemetryExceptionCounter.add(1, attributes.build());

        this.logger.PrintDebug("OpenTelemetry", "Exception Counter %s", exceptionType);
    }

    @Override
    public void recordElapsedTime(long elapsedNanos) {
        if(this.closed.get()) { return; }

        final AttributesBuilder attributes = Attributes.builder();
        attributes.putAll(this.hbAttributes[0]);

        this.openTelemetryLatencyMSHistogram.record((double) elapsedNanos / Helpers.NS_TO_MS,
                                                    attributes.build());

        this.logger.PrintDebug("OpenTelemetry", "Elapsed Time Record  %s %s", workloadName, wlTypeStage);
    }

    private void pendingTransCounter(long amt) {
        if(this.closed.get()) { return; }

        final AttributesBuilder attributes = Attributes.builder();
        attributes.putAll(this.hbAttributes[0]);

        this.openTelemetryPendingCounter.add(amt, attributes.build());

        this.logger.PrintDebug("OpenTelemetry", "Transaction Pending Counter %d %s %s",amt, workloadName, wlTypeStage);
    }

    @Override
    public void incrPendingTransCounter() {
        this.pendingTransCounter(1);
    }

    @Override
    public void decrPendingTransCounter() {
        this.pendingTransCounter(-1);
    }

    @Override
    public void setConnectionState(String connectionState){
        if(this.closed.get() | this.abortRun.get()) { return; }

        this.connectionState = connectionState;

        this.printDebug("Status Change  " + connectionState, false);

        this.updateInfoGauge(false);
    }

    private void setConnectionStateAbort(String state) {
        if(this.closed.get()) { return; }

        this.closed.set(true);
        this.endTimeSecs = System.currentTimeMillis() / 1000;
        this.endLocalDateTime = LocalDateTime.now();
        this.connectionState = state;

        this.printDebug(String.format("OpenTelemetry Disabled at %s", this.endLocalDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)));
        try {
            this.printDebug("Status Change  " + state, false);

            this.printMsg("Sending OpenTelemetry LUpdating Metrics in Abort Mode...", false);
            this.updateInfoGauge(false, true);
            this.printMsg("OpenTelemetry Waiting to complete... Ctrl-C to cancel OpenTelemetry update...", false);
            Thread.sleep(this.closeWaitMS + 1000); //need to wait for PROM to re-scrap...
            this.internalClose();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // Restore interrupt status
        } catch (Exception ignored) {}
        this.printMsg("Closed OpenTelemetry Exporter", false);
    }

    private void setConnectionStateClosed() {
       try {
            if (openTelemetryInfoGauge != null && !this.abortRun.get()) {
                this.endTimeSecs = System.currentTimeMillis() / 1000;
                this.endLocalDateTime = LocalDateTime.now();
                if(connectionState.equals("Running"))
                    this.connectionState = "Closed";
                this.printDebug(String.format("OpenTelemetry Disabled at %s", this.endLocalDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME)));
                this.printMsg("Sending OpenTelemetry Last Updated Metrics...", false);
                this.updateInfoGauge(false);
                Thread.sleep(this.closeWaitMS + 1000); //need to wait for PROM to re-scrap...
            }
        }
       catch (Exception ignored) {}
       finally {
            closed.set(true);
        }
    }

    private void internalClose() {

        if(openTelemetrySdk != null) {
            this.printDebug("openTelemetrySdk close....");

            this.openTelemetrySdk.close();
        }
    }

    @Override
    public boolean getClosed() { return this.closed.get(); }

    @Override
    public void close() {

        if(this.closed.get()) { return; }

        this.printDebug("Closing OpenTelemetry Exporter...");

        this.setConnectionStateClosed();

        this.internalClose();

        this.printMsg("Closed OpenTelemetry Exporter", true);
    }

    public String printConfiguration() {
        return String.format("Open Telemetry Enabled at %s%n\tPrometheus Exporter using Port %d%n\tClose wait interval %d ms%n\tScope Name: '%s'%n\tMetric Prefix Name: '%s'",
                this.startLocalDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                this.prometheusPort,
                this.closeWaitMS,
                SCOPE_NAME,
                METRIC_NAME);
    }

    @Override
    public String toString() {
        return String.format("OpenTelemetryExporter{prometheusport:%d, state:%s, Gauge:%s, transactioncounter:%s, exceptioncounter:%s, latancyhistogram:%s closed:%s}",
                                this.prometheusPort,
                                this.connectionState,
                                this.openTelemetryInfoGauge,
                                null, //this.openTelemetryTransactionCounter,
                                this.openTelemetryExceptionCounter,
                                this.openTelemetryLatencyMSHistogram,
                                this.closed.get());
    }
}
