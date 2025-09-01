package com.aerospike;

import org.apache.commons.lang3.StringUtils;
import org.javatuples.Pair;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.IVersionProvider;
import picocli.CommandLine.Parameters;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Model.OptionSpec;
import picocli.CommandLine.Model.PositionalParamSpec;
import picocli.CommandLine.ParseResult;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.jar.Attributes;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Command(name = "tinkerbench2",
        mixinStandardHelpOptions = true,
        versionProvider = TinkerBench2Args.ManifestVersionProvider.class,
        defaultValueProvider = TinkerBench2Args.DefaultProvider.class,
        description = "TinkerBench2 Query Analyzer")
public abstract class TinkerBench2Args implements Callable<Integer> {

    @Spec
    CommandSpec commandlineSpec;

    @Parameters(paramLabel ="QueryNameOrGremlinString",
                description = "The Gremlin query string to run or a predefined Query. "
                                + "%nIf the keyword 'List' is provided a list of predefined queries are displayed. "
                                + "%nIf a query string is provided, and Id Vertices Manager (--IdManager) is enabled, "
                                + "you can place a '%%s' or '%%d' as an vertices placeholder in the string. "
                                + "%nExample:%n\t'g.V(%%d).out().limit(5).path().by(values('code','city').fold()).tolist()'"
                                + "%n\tList -- List predefined queries"
                                + "%n\tAirRoutesQuery1 -- Predefined query for the Air Routes dataset")
    String queryNameOrString;

    @Option(names = {"-s", "--schedulers"},
            converter = SchedulerConverter.class,
            description = "The number of Schedulers to use. A value of -1 will use the default based on the number of cores. Default is ${DEFAULT-VALUE}")
    int schedulers;

    @Option(names = {"-w", "--workers"},
            converter = WorkerConverter.class,
            description = "The number of working threads per scheduler. A value of -1 will use the default based on the number of cores. Default is ${DEFAULT-VALUE}")
    int workers;

    @Option(names = {"-d", "--duration"},
            converter = DurationConverter.class,
            description = "The Time duration (wall clock) of the actual workload execution.%nThe format can be in Hour(s)|H|Hr(s), Minute(s)|M|Min(s), and/or Second(s)|S|Sec(s), ISO 8601 format (PT1H2M3.5S), or just an integer value which represents seconds.%nExample:%n\t1h30s -> One hours and 30 seconds%n\t45 -> 45 seconds...%nDefault is ${DEFAULT-VALUE}",
            defaultValue = "15M")
    Duration duration;

    @Option(names = {"-q", "-qps", "--QueriesPerSec"},
            description = "The number of queries per seconds that the workload should reach and maintain. Default is ${DEFAULT-VALUE}",
            defaultValue = "100")
    int queriesPerSecond;

    @Option(names = {"-a", "--ags", "--server"},
            description = "Specify the Aerospike Graph Server's host name or IP address.%nMultiple AGS hosts can be given by providing this option multiple times.%nExample:%n\t-a agsHost1 -a agsHost2, etc.%nDefault is '${DEFAULT-VALUE}'",
            defaultValue = "localhost")
    String[] agsHosts;

    @Option(names = {"--port"},
            description = "AGS host's port number.  Default is ${DEFAULT-VALUE}",
            defaultValue = "8182")
    int port;

    @Option(names = {"-bf", "--clusterBuildConfigFile"},
            converter = FileExistConverter.class,
            description = "A Cluster Build Configuration File. Default is ${DEFAULT-VALUE}")
    File clusterConfigurationFile;

    @Option(names = {"-b", "--clusterBuild"},
            converter = GraphConfigOptionsConverter.class,
            description = "Cluster builder options.%nMust be in the form of 'PropertyName=PropertyValue'.%nExample:%n\t-b maxConnectionPoolSize=10%n\t-b maxInProcessPerConnection=100%nYou can specify this command multiple time (one per option).")
    GraphConfigOptions[] clusterBuilderOptions;

    @Option(names = {"-wu", "-wm", "--WarmupDuration"},
            converter = DurationConverter.class,
            description = "The warmup time duration (wall clock).%nA zero duration will disable the warmup.%nThe format can be in Hour(s)|H|Hr(s), Minute(s)|M|Min(s), and/or Second(s)|S|Sec(s), ISO 8601 format (PT1H2M3.5S), or just an integer value which represents seconds.%nExample:%n\t1h30s -> One hours and 30 seconds%n\t45 -> 45 seconds...%nDefault is ${DEFAULT-VALUE}",
            defaultValue = "0")
    Duration warmupDuration;

    @Option(names = {"-g", "-t", "--gremlin"},
            converter = GraphConfigOptionsConverter.class,
            description = "Traversal configuration options.%nMust be in the form of 'PropertyName=PropertyValue'.%nExample:%n\t-g evaluationTimeout=30000%nYou can specify this command multiple time (one per option).")
    GraphConfigOptions[] gremlinConfigOptions;

    @Option(names = {"-sd","--shutdown"},
            converter = DurationConverter.class,
            description = "Additional time to wait for workload completion based on duration. Default is ${DEFAULT-VALUE}",
            defaultValue = "15S")
    Duration shutdownTimeout;

    @Option(names = {"--prometheusPort"},
            description = "Prometheus OpenTel End Point port. If -1, OpenTel metrics are disabled. Default is ${DEFAULT-VALUE}",
            defaultValue = "19090")
    int promPort;

    @Option(names = {"-prom", "--prometheus"},
            description = "Enables Prometheus OpenTel metrics.",
            negatable = true)
    boolean promEnabled;

    @Option(names = {"-cw", "--CloseWait"},
            converter = DurationConverter.class,
            description = "Close wait interval used upon application exit. This interval ensure all values are picked up by the Prometheus server. This should match the 'scrape_interval' in the PROM ymal file. Can be zero to disable wait. Default is ${DEFAULT-VALUE}",
            defaultValue = "15S")
    Duration closeWaitDuration;

    @Option(names = {"-id", "--IdManager"},
            converter = IdManagerConverter.class,
            description = "The IdManager to use for the workload. Default is ${DEFAULT-VALUE}",
            defaultValue = "com.aerospike.IdSampler")
    IdManager idManager;

    @Option(names = {"-sample", "--IdSampleSize"},
            description = "The Id sample size of vertices used by the IdManager. Zero to disable. Default is ${DEFAULT-VALUE}",
            defaultValue = "500000")
    int idSampleSize;

    @Option(names = {"-label", "--IdSampleLabel"},
            description = "The Label used to obtain Id samples used by the IdManager. Null to obtain the vertices based on the Id sample size.")
    String labelSample;

    @Option(names = {"-e","--Errors"},
            description = "The number of errors the workload can encounter before it is aborted. Default is ${DEFAULT-VALUE}",
            defaultValue = "150")
    int errorsAbort;

    @Option(names = {"-debug"},
                description = "Enables application debug tracing.")
    boolean debug;

    @Option(names = {"-AppTest"},
            hidden = true,
            description = "Enables application test mode (disabled any connections to AGS).")
    boolean appTestMode;

    @Option(names = {"-r", "--result"},
            negatable = true,
            description = "If provided, the results of the  Gremlin termination step are displayed and logged")
    public boolean printResult;

    @Option(names = {"-hg", "--HdrHistFmt"},
            negatable  = true,
            description = "If provided, the HdrHistogram Latency format is printed to the console.")
    public boolean hdrHistFmt;

    @Option(names = "-background",
            negatable  = false,
            description = "If provided, certain console output (e.g., progression bar) is suppressed.")
    public boolean backgroundMode;

    public final AtomicBoolean abortRun = new AtomicBoolean(false);
    public final AtomicBoolean abortSIGRun = new AtomicBoolean(false);
    public final AtomicBoolean terminateRun = new AtomicBoolean(false);

    /**
     * {@link IVersionProvider} implementation that returns version information from {@code /MANIFEST.MF} and {@code /META-INF/MANIFEST.MF} file.
     */
    static final class ManifestVersionProvider implements IVersionProvider {

        public String[] getVersion() throws Exception {
            final List<String> versions = new ArrayList<>();

            final String title = getClass().getPackage().getImplementationTitle();
            final String version = getClass().getPackage().getImplementationVersion();

            if (title != null && version != null) {
                versions.add(title + " version \"" + version + "\"");
            }
            else {
                getVersionsFromMetaInf(TinkerBench2Args.class.getClassLoader().getResources("META-INF/MANIFEST.MF"),
                                        "TinkerBench2",
                                        "com.aerospike.Main",
                                        versions);
                if(versions.isEmpty()) {
                    versions.add("TinkerBench2");
                }
            }

            getVersionsFromMetaInf(CommandLine.class.getClassLoader().getResources("META-INF/MANIFEST.MF"),
                                    "Apache TinkerPop",
                                    "tinkergraph-gremlin",
                                    versions);

            return versions.toArray(new String[0]);
        }

        private void getVersionsFromMetaInf(Enumeration<URL> resources,
                                            String packageName,
                                            String className,
                                            List<String> versions) {

            while (resources.hasMoreElements()) {
                URL url = resources.nextElement();

                try {
                    Manifest manifest = new Manifest(url.openStream());
                    if ((packageName != null
                                && isApplicableManifest(manifest, packageName))
                            || (className != null
                                    && isApplicableClass(manifest, className))) {
                        Attributes attr = manifest.getMainAttributes();
                        Object title = get(attr, "Implementation-Title");
                        Object version = get(attr, "Implementation-Version");
                        if (title != null && version != null) {
                            versions.add(title + " version \"" +  version +  "\"");
                        }
                    } else {
                        Pair<String,String> result = isClassPath(manifest, className);
                        if (result != null) {
                            versions.add(result.getValue0() + " version \"" +  result.getValue1() +  "\"");
                        }
                    }
                } catch (IOException ex) {
                    versions.add("Unable to read from " + url + ": " + ex);
                }
            }
        }

        private boolean isApplicableManifest(Manifest manifest,
                                                String packageName) {
            Attributes attributes = manifest.getMainAttributes();
            Object title = get(attributes, "Implementation-Title");
            return title != null && ((String)title).startsWith(packageName);
        }

        private boolean isApplicableClass(Manifest manifest,
                                             String className) {
            Attributes attributes = manifest.getMainAttributes();
            Object classValue = get(attributes, "Implementation-Class");
            if (classValue == null) {
                classValue = get(attributes, "Main-Class");
            }
            return classValue != null && classValue.equals(className);
        }

        private Pair<String,String> isClassPath(Manifest manifest,
                                                String className) {
            Attributes attributes = manifest.getMainAttributes();
            Object classValue = get(attributes, "Class-Path");

            if (classValue != null) {
                Pattern pattern = Pattern.compile(String.format("%s-(?<ver>\\d+\\.\\d+(?:\\.\\d+))\\.jar", className));
                Matcher match = pattern.matcher((String)classValue);
                if (match.find()) {
                    return new Pair<>(className, match.group("ver"));
                }
            }

            return null;
        }

        private static Object get(Attributes attributes, String key) {
            return attributes.get(new Attributes.Name(key));
        }
    }

    static final class DefaultProvider extends picocli.CommandLine.PropertiesDefaultProvider {

        public static final int nbrCores = Runtime.getRuntime().availableProcessors();

        public DefaultProvider() {
            super() ;
        }
        public DefaultProvider(File file) { super(file); }

        public static int DefaultNbrSchedules() { return (int) Math.floor(nbrCores * .25); }
        public static int DefaultNbrWorks() { return (int) Math.ceil(nbrCores * .5); }

        @Override
        public String defaultValue(CommandLine.Model.ArgSpec argSpec) throws Exception {

            String defaultValue = super.defaultValue(argSpec);

            if (argSpec.isOption()) {
                OptionSpec option = (OptionSpec) argSpec;
                if(StringUtils.isEmpty(defaultValue)
                        || defaultValue.equals("0")
                        || defaultValue.equals("-1")) {
                    if ("--schedulers".equals(option.longestName())) {
                        return String.valueOf(DefaultNbrSchedules());
                    } else if ("--workers".equals(option.longestName())) {
                        return String.valueOf(DefaultNbrWorks());
                    }
                }
            }
            return defaultValue;
        }
    }

    static final class SchedulerConverter implements CommandLine.ITypeConverter<Integer> {
        @Override
        public Integer convert(String value) throws Exception {
            int i = Integer.parseInt(value);

            if(i <= 0) {
                i = DefaultProvider.DefaultNbrSchedules();
            }

            return i;
        }
    }

    static final class WorkerConverter implements CommandLine.ITypeConverter<Integer> {
        @Override
        public Integer convert(String value) throws Exception {
            int i = Integer.parseInt(value);

            if(i <= 0) {
                i = DefaultProvider.DefaultNbrWorks();
            }

            return i;
        }
    }

    static final class DurationConverter implements CommandLine.ITypeConverter<Duration> {

        final Pattern timePattern = Pattern.compile("(?:(?<hour>\\d+(\\.\\d+)?)\\s*(?:hours?|hrs?|h))?\\s*(?:(?<min>\\d+(\\.\\d+)?)\\s*(?:minutes?|mins?|m))?\\s*(?:(?<sec>\\d+(\\.\\d+)?)\\s*(?:seconds?|secs?|s))?",
                                                        Pattern.CASE_INSENSITIVE);

        public static int isNumeric(String str) {
            if (str == null || str.isEmpty()) {
                return 0;
            }
            try {
                return Integer.parseInt(str);
            } catch (NumberFormatException e) {
                return -1;
            }
        }

        @Override
        public Duration convert(String value) throws Exception {
            if(value.startsWith("PT") || value.startsWith("pt")) {
                return Duration.parse(value.toUpperCase());
            }
            int dSec = isNumeric(value);
            if(dSec >= 0)
                return Duration.ofSeconds(dSec);
            Matcher m = timePattern.matcher(value);
            if(m.matches()) {
                String isoTimeStr = "PT";
                if(m.group("hour") != null) {
                    isoTimeStr += m.group("hour") + "H";
                }
                if(m.group("min") != null) {
                    isoTimeStr += m.group("min") + "M";
                }
                if(m.group("sec") != null) {
                    isoTimeStr += m.group("sec") + "S";
                }
                return Duration.parse(isoTimeStr);
            }
            throw new DateTimeParseException("Text cannot be parsed to a Duration. Must be in a form of #H#M#S where # is a number.", value, 0);
        }
    }

    static final class IdManagerConverter implements CommandLine.ITypeConverter<IdManager> {
        @Override
        public IdManager convert(String value) throws IllegalArgumentException {
            try {
                Class<?> idManagerClass = Class.forName(value);
                if (IdManager.class.isAssignableFrom(idManagerClass)) {
                    return (IdManager) idManagerClass.getDeclaredConstructor().newInstance();
                } else {
                    throw new IllegalArgumentException("Class " + value + " does not implement IdManager interface.");
                }
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to instantiate IdManager from class: " + value, e);
            }
        }
    }

    static final class FileExistConverter implements CommandLine.ITypeConverter<File> {
        @Override
        public File convert(String value) throws IllegalArgumentException, FileNotFoundException {
            if(value == null || value.isEmpty()) { return null; }

            File file;
            try {
               file = new File(value);
            } catch (Exception e) {
                throw new IllegalArgumentException("Failed to instantiate a file from: " + value, e);
            }
            if(!file.exists()) {
                throw new FileNotFoundException("File " + value + " does not exist.");
            }
            return file;
        }
    }

    static final class GraphConfigOptionsConverter implements CommandLine.ITypeConverter<GraphConfigOptions> {
        @Override
        public GraphConfigOptions convert(String value) throws IllegalArgumentException, FileNotFoundException {
            if(value == null || value.isEmpty()) { return null; }

            return GraphConfigOptions.Create(value);
        }
    }

    static final class AerospikeConfigOptionsConverter implements CommandLine.ITypeConverter<GraphConfigOptions> {
        @Override
        public GraphConfigOptions convert(String value) throws IllegalArgumentException, FileNotFoundException {
            if(value == null || value.isEmpty()) { return null; }

            if(value.startsWith("aerospike")) {
                return GraphConfigOptions.Create(value);
            } else if (value.charAt(0) == '.') {
                return GraphConfigOptions.Create("aerospike" +value);
            }
            return GraphConfigOptions.Create("aerospike." + value);
        }
    }

    String[] getVersions(Boolean onlyApplicationVersion) {
        ManifestVersionProvider provider = new ManifestVersionProvider();
        try {
            return provider.getVersion();
        }
        catch (Exception e) {
            return onlyApplicationVersion ? new String[] {"N/A"} : new String[0];
        }
    }

    private boolean missing(List<?> list) {
        return list == null || list.isEmpty();
    }

    private boolean missing(String value) {
        return value == null || value.isEmpty();
    }

    private boolean missing(File value) {
        return value == null;
    }

    public void validate() {

        if(missing(queryNameOrString)){
            throw new CommandLine.ParameterException(commandlineSpec.commandLine(),
                    "Argument Query string or Query Name ('queryNameOrString') cannot be null");
        }

        if(duration.isZero() || duration.isNegative()) {
            throw new CommandLine.ParameterException(commandlineSpec.commandLine(),
                    "Argument duration cannot be zero or negative.");
        }

        if(warmupDuration.isNegative()) {
            throw new CommandLine.ParameterException(commandlineSpec.commandLine(),
                    "Argument warmup cannot be negative.");
        }

        if(closeWaitDuration.isZero() || closeWaitDuration.isNegative()) {
            throw new CommandLine.ParameterException(commandlineSpec.commandLine(),
                    "Argument Close Wait cannot be zero or negative.");
        }

        if(shutdownTimeout.isZero() || shutdownTimeout.isNegative()) {
            throw new CommandLine.ParameterException(commandlineSpec.commandLine(),
                    "Argument Shutdown duration cannot be zero or negative.");
        }

        if(queriesPerSecond <= 0) {
            throw new CommandLine.ParameterException(commandlineSpec.commandLine(),
                    "Argument Queries-per-Second cannot be zero or negative.");
        }

        if(port <= 0) {
            throw new CommandLine.ParameterException(commandlineSpec.commandLine(),
                    "Argument AGS port cannot be zero or negative.");
        }

        if(errorsAbort <= 0) {
            throw new CommandLine.ParameterException(commandlineSpec.commandLine(),
                    "Argument number of Errors to Abort cannot be zero or negative.");
        }

        if (labelSample != null
                   && labelSample.equalsIgnoreCase("null")) {
            labelSample = null;
        }
        if (labelSample != null
                && labelSample.isEmpty()) {
            labelSample = null;
        }

        if(!missing(clusterConfigurationFile) && !clusterConfigurationFile.exists()) {
            throw new CommandLine.ParameterException(commandlineSpec.commandLine(),
                    "File " + clusterConfigurationFile + " doesn't exist for option 'clusterBuildConfigFile'");
        }

        ParseResult pr = commandlineSpec.commandLine().getParseResult();
        Map<String,OptionSpec> opts = commandlineSpec
                                    .options()
                                    .stream()
                                    .collect(Collectors.toMap(OptionSpec::longestName, o -> o));

        if(!missing(clusterConfigurationFile)) {
            OptionSpec item = opts.get("ags");
            boolean usesDefaultValue= !pr.hasMatchedOption(item);

            if(!usesDefaultValue) {
                throw new CommandLine.ParameterException(commandlineSpec.commandLine(),
                        "Cluster build configuration file was provided but the AGS Host(s) was/were also provided. "
                        + "You can only supply the config file or the AGS host argument(s), not both!");
            }

            item = opts.get("port");
            usesDefaultValue= !pr.hasMatchedOption(item);

            if(!usesDefaultValue) {
                throw new CommandLine.ParameterException(commandlineSpec.commandLine(),
                        "Cluster build configuration file was provided but AGS port number was also provided. "
                                + "You can only supply the config file or the AGS port argument, not both!");
            }
        }

        if(queryNameOrString.equals(com.aerospike.predefined.TestRun.class.getSimpleName())) {
            appTestMode=true;
        }
    }

    public boolean ListPredefinedQueries() {

        if(!missing(queryNameOrString)
                && queryNameOrString.equalsIgnoreCase("list")) {
            List<String> classes = Helpers.findAllPredefinedQueries("com.aerospike.predefined");
            if(classes.isEmpty()) {
                System.err.println("There were no predefined queries found.");
            } else {
                Helpers.Println(System.out,
                            "Following is a list of Predefined Queries:",
                                Helpers.BLACK,
                                Helpers.YELLOW_BACKGROUND);
                for(String className : classes) {
                    String queryName = className;
                    try {
                        final QueryRunnable instance = Helpers.GetQuery(className, null, null, null, false);
                        queryName = instance.Name();
                        if(instance.getDescription() != null) {
                            queryName += String.format(" -- %s", instance.getDescription());
                            queryName = queryName.replace("\t", "\t\t");
                        }
                        if(instance.getSampleLabelId() != null) {
                            queryName += String.format("%n\t\tSample Vertices id label of '%s'", instance.getSampleLabelId());
                        }
                    } catch (Exception ignored) {}

                    Helpers.Print(System.out,
                                    String.format("\t%s%n", queryName),
                                    Helpers.BLACK,
                                    Helpers.GREEN_BACKGROUND);
                }
            }
            return true;
        }
        return false;
    }

    /*
    Prints to System out the arguments provided or default values based on {@code onlyProvidedArgs}.
    @param onlyProvidedArgs -- If true, the arguments given in the command line are printed.
        if false, All provided arguments including defaulted values are printed.
     */
    void PrintArguments(boolean onlyProvidedArgs) {
        ParseResult pr = commandlineSpec.commandLine().getParseResult();

        System.out.println("Arguments:");
        String[] args = getArguments(onlyProvidedArgs);

        if(args.length > 0) {
            for (String arg : args) {
                System.out.printf("\t%s%n", arg);
            }
        } else {
            System.out.println("\tNo arguments were provided.");
        }
    }

    /*
    Gets the arguments provided or default values based on {@code onlyProvidedArgs}.
    @param onlyProvidedArgs -- If true, the arguments given in the command line are printed.
        if false, All provided arguments including defaulted values are printed.
     */
    String[] getArguments(boolean onlyProvidedArgs) {
        ParseResult pr = commandlineSpec.commandLine().getParseResult();

        List<String> args = new ArrayList<>();

        List<PositionalParamSpec> positional = commandlineSpec.positionalParameters();
        for (PositionalParamSpec argPos : positional) {
            if(!argPos.hidden()) {
                String argKeyword = argPos.paramLabel();
                Object value = argPos.getValue();
                if (value.getClass().isArray())
                    value = Arrays.toString((Object[]) value);

                args.add(String.format("%s (Position %s): %s",
                                        argKeyword,
                                        argPos.index(),
                                        value));
            }
        }

        List<OptionSpec> options = commandlineSpec.options();
        for (OptionSpec opt : options) {
            String argKeyword = opt.longestName();
            if(argKeyword.equals("--help")
                    || argKeyword.equals("--version")
                    || opt.hidden()) {
                continue;
            }

            boolean usesDefaultValue= !pr.hasMatchedOption(opt);
            if(onlyProvidedArgs && usesDefaultValue) {
                continue;
            }

            Object value = opt.getValue();
            if(value != null && value.getClass().isArray())
                value = Arrays.toString((Object[]) value);
            args.add(String.format("%s: %s%s",
                        argKeyword,
                        value,
                        (usesDefaultValue) ? " (Default)" : ""));
        }

        return args.toArray(new String[0]);
    }
}
