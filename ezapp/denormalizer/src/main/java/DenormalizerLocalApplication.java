import joptsimple.OptionSet;
import org.apache.samza.config.Config;
import org.apache.samza.runtime.LocalApplicationRunner;
import org.apache.samza.util.CommandLine;
import org.ezstack.denormalizer.core.DenormalizerApp;

public class DenormalizerLocalApplication {

    public static void main(String[] args) {

        CommandLine cmdLine = new CommandLine();
        OptionSet options = cmdLine.parser().parse(args);
        Config config = cmdLine.loadConfig(options);

        LocalApplicationRunner runner = new LocalApplicationRunner(config);

        DenormalizerApp app = new DenormalizerApp();

        runner.run(app);
        runner.waitForFinish();

     }
}
