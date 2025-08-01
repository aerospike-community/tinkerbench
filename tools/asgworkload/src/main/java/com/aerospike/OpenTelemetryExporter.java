package com.aerospike;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.*;
import io.opentelemetry.exporter.prometheus.PrometheusHttpServer;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.resources.Resource;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.opentelemetry.semconv.ServiceAttributes.SERVICE_NAME;

public final class OpenTelemetryExporter implements com.aerospike.OpenTelemetry {

    private final String SCOPE_NAME = "com.aerospike.workload";
    private final String METRIC_NAME = "aerospike.workload.ags";

    private final int prometheusPort;
    private String workloadName;
    private String connectionState;
    private String wlType = "";
    private final long pid = ProcessHandle.current().pid();
    private final int closeWaitMS;

    private final Attributes[] hbAttributes;
    private final OpenTelemetrySdk openTelemetrySdk;
    private final LongGauge openTelemetryInfoGauge;
    private final LongCounter openTelemetryExceptionCounter;
    private final LongCounter openTelemetryTransactionCounter;
    private final DoubleHistogram openTelemetryLatencyMSHistogram;
    private final DoubleHistogram openTelemetryLatencyUSHistogram;

    private final AtomicInteger hbCnt = new AtomicInteger();
    private final long startTimeMillis;
    private final LocalDateTime startLocalDateTime;
    private long endTimeMillis;
    private LocalDateTime endLocalDateTime;
    private Duration targetDuration;
    private final AtomicBoolean closed = new AtomicBoolean(false);

    private final LogSource logger = LogSource.getInstance();

    public final AtomicBoolean abortRun;
    public final AtomicBoolean terminateRun;

    public OpenTelemetryExporter(AGSWorkloadArgs args,
                                 StringBuilder otherInfo) {

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

        this.openTelemetryExceptionCounter =
                openTelemetryMeter
                    .counterBuilder(METRIC_NAME + ".exception")
                        .setDescription("Aerospike Workload Exception")
                        .build();

        //this.openTelemetryErrorCounter =
        //        openTelemetryMeter
        //                .counterBuilder(METRIC_NAME + ".related.errors")
        //                .setDescription("Aerospike Workload Related Errors")
        //                .build();

        this.openTelemetryLatencyMSHistogram =
                openTelemetryMeter
                        .histogramBuilder(METRIC_NAME + ".lng.latency")
                        .setDescription("Aerospike Workload Latencies (ms)")
                        .setUnit("ms")
                        .build();

        this.openTelemetryLatencyUSHistogram =
                openTelemetryMeter
                        .histogramBuilder(METRIC_NAME + ".latency")
                        .setDescription("Aerospike Workload Latencies (us)")
                        .setUnit("microsecond")
                        .build();

        this.openTelemetryTransactionCounter =
                openTelemetryMeter
                        .counterBuilder(METRIC_NAME + ".transaction")
                        .setDescription("Aerospike Workload Transaction")
                        .build();

        this.printDebug("SDK and Metrics Completed");
        this.printDebug("Register to Counters");
        this.printDebug("Updating Gauge");

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
        } catch (InterruptedException ignored) {
        }
    }

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

            System.out.printf("%s %s\n", formattedDateTime, msg);
        }
        else {
            System.out.printf("%s\n", msg);
        }
    }

    private void updateInfoGauge(boolean initial) {
        this.updateInfoGauge(initial, false);
    }

    private void updateInfoGauge(boolean initial, boolean force) {

        if(!force && this.closed.get()) { return; }

        AttributesBuilder attributes = Attributes.builder();

        //attributes.putAll(this.hbAttributes[0]);
        //attributes.put("hbCount", this.hbCnt.incrementAndGet());

        //if(initial) {
            attributes.put("type", "static");
            for (Attributes attrItem : this.hbAttributes) {
                attributes.putAll(attrItem);
            }
        //}
        if(this.connectionState != null) {
            attributes.put("DBConnState", this.connectionState);
        }
        if(this.endTimeMillis != 0) {
            attributes.put("endTimeMillis", this.endTimeMillis);
            attributes.put("endLocalDateTime", this.endLocalDateTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
            attributes.put("runDurationMillis", this.endTimeMillis - this.startTimeMillis);
        }

        long hCnt = (long) hbCnt.incrementAndGet();
        if(hCnt >= 90) {
            hCnt = 0;
            hbCnt.set(0);
        }
        long counter = (System.currentTimeMillis()
                                * 100L)
                            + hCnt ;
        this.openTelemetryInfoGauge.set(counter,
                                        attributes.build());

       this.printDebug(String.format("Info Gauge %d", hbCnt.get()));
    }

    public void Reset(AGSWorkloadArgs args,
                      String workloadName,
                      String workloadType,
                      Duration targetDuration,
                      boolean warmup,
                      StringBuilder otherInfo) {

        this.wlType = String.format("%s-%s",
                                    warmup ? "Warmup" : "Workload",
                                    workloadType == null ? "Initial" : workloadType);
        this.workloadName = workloadName == null ? "Workload" : workloadName;

        this.endTimeMillis = 0;
        this.endLocalDateTime = null;
        this.targetDuration = targetDuration;

        this.hbAttributes[0] =
                Attributes.of(
                        AttributeKey.stringKey("workload"), this.workloadName,
                        AttributeKey.stringKey("wltype"), this.wlType,
                        AttributeKey.longKey("pid"), this.pid
                );
                //There are attributes commonly used for all events
        this.hbAttributes[1] =
                Attributes.of(
                        AttributeKey.stringKey("otherInfo"), otherInfo == null ? null : otherInfo.toString(),
                        AttributeKey.longKey("startTimeMillis"), this.startTimeMillis,
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
                        AttributeKey.longKey("CallsPerSecond"), (long) args.callsPerSecond,
                        AttributeKey.longKey("schedulars"), (long) args.schedulers,
                        AttributeKey.longKey("workers"), (long) args.workers,
                        AttributeKey.stringKey("duration"), targetDuration.toString(),
                        AttributeKey.longKey("durationMillis"), targetDuration.toMillis()
                );

        this.updateInfoGauge(true);
    }

    @Override
    public void addException(Exception exception) {
        if(this.closed.get()) { return; }

        final String exceptionType = Helpers.GetShortClassName(exception.getClass().getName());
        final String message = Helpers.GetShortErrorMsg(exception.getMessage(), 160);

        this.addException(exceptionType, message);
    }

    @Override
    public void addException(String exceptionType, String message) {

        if(this.closed.get()) { return; }

        //Ignore Cluser Closed exceptions when the app is terminating
        if((terminateRun.get() || abortRun.get()) && Objects.equals(message, "Cluster has been closed")) {
            return;
        }

        AttributesBuilder attributes = Attributes.builder();
        attributes.putAll(this.hbAttributes[0]);
        attributes.putAll(Attributes.of(
                AttributeKey.stringKey("exception_type"), exceptionType,
                AttributeKey.stringKey("exception"), message,
                AttributeKey.longKey("startTimeMillis"), this.startTimeMillis
        ));

        this.openTelemetryExceptionCounter.add(1, attributes.build());

        this.printDebug("Exception Counter Add " + exceptionType);
    }

    @Override
    public void recordElapsedTime(long elapsedNanos) {
        if(this.closed.get()) { return; }

        AttributesBuilder attributes = Attributes.builder();
        attributes.putAll(this.hbAttributes[0]);

        attributes.put("startTimeMillis", this.startTimeMillis);
        attributes.put("targetdurationMills", this.targetDuration.toMillis());

        final Attributes attrsBuilt = attributes.build();

        this.openTelemetryLatencyMSHistogram.record((double) elapsedNanos / Helpers.NS_TO_MS, attrsBuilt);
        this.openTelemetryLatencyUSHistogram.record((double) elapsedNanos / Helpers.NS_TO_US, attrsBuilt);
        this.openTelemetryTransactionCounter.add(1, attrsBuilt);

        this.printDebug("Elapsed Time Record  " + workloadName + " " + wlType, true);
    }

    @Override
    public void incrTransCounter() {

        if(this.closed.get()) { return; }

        AttributesBuilder attributes = Attributes.builder();
        attributes.putAll(this.hbAttributes[0]);
        attributes.put("startTimeMillis", this.startTimeMillis);

        this.openTelemetryTransactionCounter.add(1, attributes.build());

        this.printDebug("Transaction Counter Add " + workloadName + " " + wlType, true);
    }

    @Override
    public void setWorkloadName(String workLoadType, String workloadName, boolean warmup) {
        if(this.closed.get() | this.abortRun.get()) { return; }

        wlType = String.format("%s-%s",
                                warmup ? "Warmup" : "Workload",
                                workLoadType);
        this.workloadName = workloadName;

        this.printDebug("Workload Name  " + workloadName + " Type " + workLoadType, false);

        this.hbAttributes[0] = Attributes.of(
                AttributeKey.stringKey("workload"), this.workloadName,
                AttributeKey.stringKey("wltype"), this.wlType,
                AttributeKey.longKey("pid"), this.pid
        );
        this.updateInfoGauge(false);
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
        this.endTimeMillis = System.currentTimeMillis();
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
                this.endTimeMillis = System.currentTimeMillis();
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
        return String.format("Open Telemetry Enabled at %s\n\tPrometheus Exporter using Port %d\n\tClose wait interval %d ms\n\tScope Name: '%s'\n\tMetric Prefix Name: '%s'",
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
                                this.openTelemetryTransactionCounter,
                                this.openTelemetryExceptionCounter,
                                this.openTelemetryLatencyUSHistogram,
                                this.closed.get());
    }
}
