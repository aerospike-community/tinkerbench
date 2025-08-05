package com.aerospike;

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

@Command(name = "agsworkload",
        mixinStandardHelpOptions = true,
        versionProvider = AGSWorkloadArgs.ManifestVersionProvider.class,
        defaultValueProvider = AGSWorkloadArgs.DefaultProvider.class,
        description = "Aerospike Graph Workload Generator")
public abstract class AGSWorkloadArgs  implements Callable<Integer> {

    @Spec
    CommandSpec commandlineSpec;

    @Parameters(description = "The Gremlin query string to run or a predefined Query. "
                                + "If a query string is provided, and Id Vertices Manager (--IdManager) is enabled, "
                                + "you can place a '%s' or '%d' as an vertices placeholder in the string. "
                                + "Example: 'g.V(%d).out().limit(5).path().by(values('code','city').fold()).tolist()'")
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
            description = "The Time duration (wall clock) of the workload. The format can be in Hour(s)|H|Hr(s), Minute(s)|M|Min(s), and/or Second(s)|S|Sec(s), ISO 8601 format (PT1H2M3.5S), or just an integer value which represents seconds. Example: 1h30s -> One hours and 30 seconds; 45 -> 45 seconds... Default is ${DEFAULT-VALUE}",
            defaultValue = "15M")
    Duration duration;

    @Option(names = {"-c", "--callsPerSec"},
            description = "The number of calls per seconds. Default is ${DEFAULT-VALUE}",
            defaultValue = "100")
    int callsPerSecond;

    @Option(names = {"-a", "--ags"},
            description = "Specify multiple AGS host by -a agsHost1 -a agsHost2, etc.  Default is '${DEFAULT-VALUE}'",
            defaultValue = "localhost")
    String[] agsHosts;

    @Option(names = {"--port"},
            description = "AGS host's port number.  Default is ${DEFAULT-VALUE}",
            defaultValue = "8182")
    int port;

    @Option(names = {"-b", "--clusterBuildConfigFile"},
            converter = FileExistConverter.class,
            description = "A Cluster Build Configuration File. Default is ${DEFAULT-VALUE}")
    File clusterConfigurationFile;

    @Option(names = {"-wu", "--WarmupDuration"},
            converter = DurationConverter.class,
            description = "The warmup time duration (wall clock). A zero duration will disable the warmup. The format can be in Hour(s)|H|Hr(s), Minute(s)|M|Min(s), and/or Second(s)|S|Sec(s), ISO 8601 format (PT1H2M3.5S), or just an integer value which represents seconds. Example: 1h30s -> One hours and 30 seconds; 45 -> 45 seconds... Default is ${DEFAULT-VALUE}",
            defaultValue = "0")
    Duration warmupDuration;

    @Option(names = {"-g", "--gremlin"},
            converter = GraphConfigOptionsConverter.class,
            description = "Aerospike or Gremlin configuration options.%nMust be in the of 'PropertyName=PropertyValue'.%nExample: -g evaluationTimeout=30000%n-g aerospike.client.policy.maxRetries=2%nYou can specify this command multiple time (one per option).%nDefault is ${DEFAULT-VALUE}")
    GraphConfigOptions[] gremlinConfigOptions;

    @Option(names = {"-as", "--aerospike"},
            converter = AerospikeConfigOptionsConverter.class,
            description = "Aerospike ONLY configuration options.%nYou are not required to include the 'aerospike' prefix to the options.%nMust be in the of 'PropertyName=PropertyValue'.%nExample: -as graph.parallelize=10%n-as client.policy.maxRetries=2%nYou can specify this command multiple time (one per option).%nDefault is ${DEFAULT-VALUE}")
    GraphConfigOptions[] asConfigOptions;

    @Option(names = {"-sd","--shutdown"},
            converter = DurationConverter.class,
            description = "Timeout used to wait for worker and scheduler shutdown. The format can be in Hour(s)|H|Hr(s), Minute(s)|M|Min(s), and/or Second(s)|S|Sec(s), ISO 8601 format (PT1H2M3.5S), or just an integer value which represents seconds. Example: 1h30s -> One hours and 30 seconds; 45 -> 45 seconds... Default is ${DEFAULT-VALUE}",
            defaultValue = "15S")
    Duration shutdownTimeout;

    @Option(names = {"--prometheusPort"},
            description = "Prometheus OpenTel End Point port. If -1, OpenTel metrics are disabled. Default is ${DEFAULT-VALUE}",
            defaultValue = "19090")
    int promPort;

    @Option(names = "-prom",
            description = "Enables Prometheus OpenTel metrics.")
    boolean promEnabled;

    @Option(names = {"-cw", "--CloseWait"},
            converter = DurationConverter.class,
            description = "Close wait interval upon application exit. This interval ensure all values are picked up by the Prometheus server. This should match the 'scrape_interval' in the PROM ymal file. Can be zero to disable wait. Default is ${DEFAULT-VALUE}",
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
            description = "The Label used to obtain Id samples used by the IdManager. Null to disable and get vertices based on the Id sample size. Default is ${DEFAULT-VALUE}")
    String labelSample;

    @Option(names = {"-e","--Errors"},
            description = "The number of errors reached when the workload is aborted. Default is ${DEFAULT-VALUE}",
            defaultValue = "150")
    int errorsAbort;

    @Option(names = "-debug",
                description = "Enables application debug tracing.")
    boolean debug;

    @Option(names = "-test",
            description = "Enables application test mode (no physical connections to AGS).")
    boolean testMode;

    @Option(names = "-result",
            description = "If provided, the results of the Query are displayed and logged")
    boolean printResult;

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
            boolean fndMainInfo = true;

            if (title != null && version != null) {
                versions.add(title + " version " + version);
                fndMainInfo = false;
            }
            else {
                getVersionsFromMetaInf(CommandLine.class.getClassLoader().getResources("MANIFEST.MF"),
                                        versions,
                                        fndMainInfo);
                fndMainInfo = versions.isEmpty();
            }

            getVersionsFromMetaInf(CommandLine.class.getClassLoader().getResources("META-INF/MANIFEST.MF"),
                                    versions,
                                    fndMainInfo);

            return versions.toArray(new String[0]);
        }

        private void getVersionsFromMetaInf(Enumeration<URL> resources,
                                            List<String> versions,
                                            boolean fndMainInfo) {

            while (resources.hasMoreElements()) {
                URL url = resources.nextElement();

                try {
                    Manifest manifest = new Manifest(url.openStream());
                    if (isApplicableManifest(manifest, "Apache TinkerPop")
                            || (fndMainInfo && isApplicableManifest(manifest, "AGS Workload"))) {
                        Attributes attr = manifest.getMainAttributes();
                        versions.add(get(attr, "Implementation-Title") +
                                        " version \"" +
                                        get(attr, "Implementation-Version") +
                                        "\"");
                    } else if (fndMainInfo && isApplicableClass(manifest, "com.aerospike.Main")) {
                        Attributes attr = manifest.getMainAttributes();
                        String aTitle = (String) get(attr, "Implementation-Title");
                        if(aTitle == null) {
                            aTitle = "AGS Workload";
                        }
                        String aVersion = (String) get(attr, "Implementation-Version");
                        if(aVersion == null) {
                            aVersion = (String) get(attr, "Manifest-Version");
                        }
                        versions.add(aTitle + " version \"" +
                                aVersion + "\"");
                        fndMainInfo = false;
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

        private static Object get(Attributes attributes, String key) {
            return attributes.get(new Attributes.Name(key));
        }
    }

    static final class DefaultProvider implements CommandLine.IDefaultValueProvider {

        private final int nbrCores = Runtime.getRuntime().availableProcessors();

        public int DefaultNbrSchedules() { return (int) Math.floor(nbrCores * .25); }
        public int DefaultNbrWorks() { return (int) Math.ceil(nbrCores * .5); }

        @Override
        public String defaultValue(CommandLine.Model.ArgSpec argSpec) throws Exception {

            if (argSpec.isOption()) {
                OptionSpec option = (OptionSpec) argSpec;
                if ("--schedulers".equals(option.longestName())) {
                    return String.valueOf(DefaultNbrSchedules());
                } else if ("--workers".equals(option.longestName())) {
                    return String.valueOf(DefaultNbrWorks());
                }
            }
            return null;
        }
    }

    static final class SchedulerConverter implements CommandLine.ITypeConverter<Integer> {
        @Override
        public Integer convert(String value) throws Exception {
            int i = Integer.parseInt(value);

            if(i <= 0) {
                DefaultProvider d = new DefaultProvider();
                i = d.DefaultNbrSchedules();
            }

            return i;
        }
    }

    static final class WorkerConverter implements CommandLine.ITypeConverter<Integer> {
        @Override
        public Integer convert(String value) throws Exception {
            int i = Integer.parseInt(value);

            if(i <= 0) {
                DefaultProvider d = new DefaultProvider();
                i = d.DefaultNbrWorks();
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

        if(callsPerSecond <= 0) {
            throw new CommandLine.ParameterException(commandlineSpec.commandLine(),
                    "Argument Call-per-Second cannot be zero or negative.");
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
    }

    /*
    Prints to System out the arguments provided or default values based on {@code onlyProvidedArgs}.
    @param onlyProvidedArgs -- If true, the arguments given in the command line are printed.
        if false, All provided arguments including defaulted values are printed.
     */
    void PrintArguments(boolean onlyProvidedArgs) {
        ParseResult pr = commandlineSpec.commandLine().getParseResult();

        System.out.println("Positional Arguments:");
        List<PositionalParamSpec> positional = commandlineSpec.positionalParameters();
        for (PositionalParamSpec argPos : positional) {
            String argKeyword = argPos.paramLabel();
            Object value = argPos.getValue();
            if(value.getClass().isArray())
                value = Arrays.toString((String[]) value);

            System.out.printf("\t%s (Position %s): %s%n",
                    argKeyword,
                    argPos.index(),
                    value);
        }

        List<OptionSpec> options = commandlineSpec.options();
        List<String> optionLst = new ArrayList<>();

        for (OptionSpec opt : options) {
            String argKeyword = opt.longestName();
            if(argKeyword.equals("--help") || argKeyword.equals("--version")) {
                continue;
            }

            boolean usesDefaultValue= !pr.hasMatchedOption(opt);
            if(onlyProvidedArgs && usesDefaultValue) {
                continue;
            }

            Object value = opt.getValue();
            if(value != null && value.getClass().isArray()) {
                value = Arrays.toString((Object[]) value);
            }
            optionLst.add(String.format("\t%s: %s%s%n",
                                            argKeyword,
                                            value,
                                            (usesDefaultValue) ? " (Default)" : ""));
        }

        if(!optionLst.isEmpty()) {
            System.out.println("Options:");
            for(String optionStr : optionLst) {
                System.out.print(optionStr);
            }
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
            String argKeyword = argPos.paramLabel();
            Object value = argPos.getValue();
            if(value.getClass().isArray())
                value = Arrays.toString((Object[]) value);

            args.add(String.format("%s (Position %s): %s",
                                    argKeyword,
                                    argPos.index(),
                                    value));
        }

        List<OptionSpec> options = commandlineSpec.options();
        for (OptionSpec opt : options) {
            String argKeyword = opt.longestName();
            if(argKeyword.equals("--help") || argKeyword.equals("--version")) {
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
