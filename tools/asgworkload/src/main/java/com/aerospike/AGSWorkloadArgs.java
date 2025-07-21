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

import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

@Command(name = "agsworkload",
        mixinStandardHelpOptions = true,
        versionProvider = AGSWorkloadArgs.ManifestVersionProvider.class,
        description = "Aerospike Graph Workload Generator")
public abstract class AGSWorkloadArgs  implements Callable<Integer> {

    @Spec
    CommandSpec commandlineSpec;

    @Parameters(description = "The query to run")
    int idxQuery;

    @Option(names = {"-s", "--schedulers"},
            defaultValue = "1",
            description = "The number of Schedulers to use. Default is ${DEFAULT-VALUE}")
    int schedulars;

    @Option(names = {"-w", "--workers"},
            defaultValue = "10",
            description = "The number of working threads per scheduler. Default is ${DEFAULT-VALUE}")
    int workers;

    @Option(names = {"-d", "--duration"},
            description = "The Run Time duration using ISO 8601 duration format (e.g., PT1H30M for 1 hour 30 minutes, PT20.30S represents a duration of 20 seconds and 300 milliseconds). Default is ${DEFAULT-VALUE}",
            defaultValue = "PT30S")
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

    @Option(names = {"-p", "--parallelize"},
            description = "Passed to Aerospike Graph Parallelize option. Default is ${DEFAULT-VALUE}",
            defaultValue = "0")
    int parallelize;

    @Option(names = {"-qhp","--queryHopPaths"},
            description = "The Number of possible query hop paths. 0 to disable. Default is ${DEFAULT-VALUE}",
            defaultValue = "0")
    int nbrHopPaths;

    @Option(names = {"-sd","--shutdown"},
            description = "Timeout used to wait for worker and scheduler shutdown. The duration is based on the ISO 8601 format (e.g., PT3M for 3 minutes, PT20.30S represents a duration of 20 seconds and 300 milliseconds). Default is ${DEFAULT-VALUE}",
            defaultValue = "PT15S")
    Duration shutdownTimeout;

    @Option(names = {"-vsf","--vertexScaleFactor"},
            description = "Scaling factor used in generating the Vertex's id. Default is ${DEFAULT-VALUE}",
            defaultValue = "1")
    Long vertexScaleFactor;

    @Option(names = {"--vertexIdFormat"},
            description = "The Vertex's id format where a random value based on Vertex Scale Factor. Default is ${DEFAULT-VALUE}",
            defaultValue = "A%019d")
    String vertexIdFmt;

    @Option(names = {"-prom", "--prometheusPort"},
            description = "Prometheus OpenTel End Point port. If -1, OpenTel metrics are disabled. Default is ${DEFAULT-VALUE}",
            defaultValue = "19090")
    int promPort;

    @Option(names = {"-promWait", "--prometheusCloseWaitSecs"},
            description = "Open Telemetry wait interval, in seconds, upon application exit. This interval ensure all values are picked by the Prometheus server. This should match the 'scrape_interval' in the PROM ymal file. Can be zero to disable wait. Default is ${DEFAULT-VALUE}",
            defaultValue = "15")
    int promCloseWaitSecs;

    @Option(names = "-debug")
    boolean debug;

    /**
     * {@link IVersionProvider} implementation that returns version information from {@code /MANIFEST.MF} and {@code /META-INF/MANIFEST.MF} file.
     */
    static class ManifestVersionProvider implements IVersionProvider {

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

    String[] getVersions(Boolean onlyApplicationVersion) {
        ManifestVersionProvider provider = new ManifestVersionProvider();
        try {
            return provider.getVersion();
        }
        catch (Exception e) {
            return onlyApplicationVersion ? new String[] {"N/A"} : new String[0];
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
            if(value.getClass().isArray())
                value = Arrays.toString((String[]) value);
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
                value = Arrays.toString((String[]) value);

            args.add(String.format("%s (Position %s): %s%n",
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
            if(value.getClass().isArray())
                value = Arrays.toString((String[]) value);
            args.add(String.format("%s: %s%s%n",
                        argKeyword,
                        value,
                        (usesDefaultValue) ? " (Default)" : ""));
        }

        return args.toArray(new String[0]);
    }
}
