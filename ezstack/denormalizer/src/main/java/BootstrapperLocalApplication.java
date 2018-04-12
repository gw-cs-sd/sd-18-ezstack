import joptsimple.OptionSet;
import org.apache.samza.config.Config;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.util.CommandLine;
import org.ezstack.denormalizer.bootstrapper.BootstrapperApp;

public class BootstrapperLocalApplication {

    public static void main(String[] args) {

        CommandLine cmdLine = new CommandLine();
        OptionSet options = cmdLine.parser().parse(args);
        Config config = cmdLine.loadConfig(options);

        LocalApplicationRunner runner = new LocalApplicationRunner(config);

        BootstrapperApp app = new BootstrapperApp();

        runner.run(app);
        runner.waitForFinish();

    }
}
