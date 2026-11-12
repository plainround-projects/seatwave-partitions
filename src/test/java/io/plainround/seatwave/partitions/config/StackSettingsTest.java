package io.plainround.seatwave.partitions.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.github.sviperll.result4j.Result;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import org.junit.jupiter.api.Test;

class StackSettingsTest {

  @Test
  void readsTheShippedSettingsFileWithBothMachinesInFileOrder() throws IOException {
    Result<StackSettings, List<String>> read = StackSettings.read(reader(shipped()));

    assertThat(read.discardError())
        .hasValueSatisfying(settings ->
            assertThat(settings.hostNames()).containsExactly("shard-host-1", "shard-host-2"));
  }

  @Test
  void refusesAMachineThatIsNotOnTheLoopbackAddress() throws IOException {
    Properties settings = shipped();
    settings.setProperty("host.shard-host-2.url", "jdbc:postgresql://0.0.0.0:21802/seatwave");

    assertThat(problems(settings))
        .singleElement()
        .asString()
        .startsWith("host.shard-host-2.url must match");
  }

  @Test
  void refusesASingleMachineBecauseAMoveNeedsTwo() throws IOException {
    Properties settings = shipped();
    settings.setProperty("hosts", "shard-host-1");

    assertThat(problems(settings))
        .containsExactly("hosts must name at least two shard machines, but named 1");
  }

  @Test
  void reportsAMissingHostListOnceWithoutTheTwoMachineRule() throws IOException {
    Properties settings = shipped();
    settings.remove("hosts");

    assertThat(problems(settings)).containsExactly("hosts is missing");
  }

  @Test
  void reportsEveryProblemTogether() throws IOException {
    Properties settings = shipped();
    settings.setProperty("db.poolSize", "0");
    settings.setProperty("seed.sections", "25001");
    settings.remove("lease.durationSeconds");

    assertThat(problems(settings))
        .containsExactly(
            "db.poolSize must be from 1 to 64, but was 0",
            "lease.durationSeconds is missing",
            "seed.sections must be from 1 to 25000, but was 25001");
  }

  private static Properties shipped() throws IOException {
    return SettingsReader.load(Path.of("config/stack.properties"));
  }

  private static SettingsReader reader(Properties properties) {
    return new SettingsReader(properties);
  }

  private static List<String> problems(Properties properties) {
    return switch (StackSettings.read(reader(properties))) {
      case Result.Success<StackSettings, List<String>> _ -> List.of();
      case Result.Error<StackSettings, List<String>>(List<String> problems) -> problems;
    };
  }
}
