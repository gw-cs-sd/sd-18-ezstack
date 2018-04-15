
import joptsimple.OptionSet;
import org.apache.samza.config.Config;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.util.CommandLine;
import org.ezstack.deity.DenormalizationDeityApp;

public class DenormalizationDeityLocalRunner {
    public static void main(String[] args) {

        CommandLine cmdLine = new CommandLine();
        OptionSet options = cmdLine.parser().parse(args);
        Config config = cmdLine.loadConfig(options);

        LocalApplicationRunner runner = new LocalApplicationRunner(config);

        DenormalizationDeityApp app = new DenormalizationDeityApp();

        runner.run(app);
        runner.waitForFinish();

    }
}
