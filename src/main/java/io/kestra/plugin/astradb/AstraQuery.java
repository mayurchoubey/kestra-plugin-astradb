package io.kestra.plugin.astradb;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import io.kestra.core.models.annotations.Plugin;
import io.kestra.core.models.executions.metrics.Counter;
import io.kestra.core.serializers.FileSerde;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.*;
import lombok.experimental.SuperBuilder;
import io.kestra.core.models.annotations.PluginProperty;
import io.kestra.core.models.tasks.RunnableTask;
import io.kestra.core.models.tasks.Task;
import io.kestra.core.runners.RunContext;

import javax.validation.constraints.NotNull;
import java.io.*;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.datastax.oss.driver.api.core.cql.ResultSet;

import static io.kestra.core.utils.Rethrow.throwConsumer;

@SuperBuilder
@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
@Schema(
    title = "Query an Astra database with CQL.",
    description = "Task to execute query on Datastax Astra DB (Serverless, fully managed)"
)

@Plugin(
        examples = {
                @io.kestra.core.models.annotations.Example(
                        title = "Send a cql query to an Astra Database",
                        code = {
                                "secureBundle : securebundle zip file",
                                "keyspace: keyspace to connect",
                                "clientId: clientId from Astra token json file",
                                "clientSecrets: secrets from Astra token json file",
                                "cql: SELECT * FROM CQL_KEYSPACE.CQL_TABLE",
                        }
                ),
        }
)
public class AstraQuery extends Task implements RunnableTask<AstraOutput>, IQueryable {
    @Schema(
        title = "The Astra DB session connection configurations",
        description = "The Astra DB session connection configurations"
    )
    @PluginProperty(dynamic = true)
    @NotNull
    protected String secureBundle;

    @PluginProperty
    @NotNull
    protected String keyspace = "";

    @PluginProperty
    @NotNull
    protected String clientId = "";

    @PluginProperty
    @NotNull
    protected String clientSecrets = "";

    @PluginProperty
    @NotNull
    protected String cql = "";

    @Builder.Default
    private boolean fetch = false;

    @Builder.Default
    private boolean store = false;

    @Builder.Default
    private boolean fetchOne = false;

    @Override
    public AstraOutput run(RunContext runContext) throws Exception {
        // variable initialization. in future from UI.
        String keyspace = runContext.render(this.keyspace);
        String clientId = runContext.render(this.clientId);
        String clientSecrets = runContext.render(this.clientSecrets);
        String secureBundlePath = runContext.render(this.secureBundle);
        String cql = runContext.render(this.cql).trim();

        // Create the Astra CqlSession object:
        try (CqlSession session = CqlSession.builder()
                .withCloudSecureConnectBundle(runContext.uriToInputStream(new URI(secureBundlePath)))
                .withAuthCredentials(clientId, clientSecrets)
                .withKeyspace(keyspace)
                .build()) {

            // Execute the cql query
            ResultSet rs = session.execute(cql);
            ColumnDefinitions columnDefinitions = rs.getColumnDefinitions();
            AstraOutput.AstraOutputBuilder astraOutputBuilder;
            astraOutputBuilder = AstraOutput.builder()
                    .bytes(rs.getExecutionInfo().getResponseSizeInBytes());

            // handle various scenarios
            if (this.fetchOne) {
                handleFetchOne(rs, columnDefinitions, astraOutputBuilder);
            } else if (this.store) {
                handleFetchStore(runContext, rs, columnDefinitions, astraOutputBuilder);
            } else if (this.fetch) {
                handleFetchWithLimit(rs, columnDefinitions, astraOutputBuilder);
            }
            // building the output of the task
            AstraOutput output = astraOutputBuilder.build();

            if (output.getSize() != null) {
                runContext.metric(Counter.of("fetch.size", output.getSize()));
            }

            if (output.getBytes() != null) {
                runContext.metric(Counter.of("fetch.size", output.getBytes()));
            }

            return output;
        }
    }

    private static void handleFetchWithLimit(ResultSet rs, ColumnDefinitions columnDefinitions, AstraOutput.AstraOutputBuilder outputBuilder) {
        List<Map<String, Object>> maps = new ArrayList<>();
        rs.forEach(row -> maps.add(AstraHelper.convertRow(row, columnDefinitions)));

        outputBuilder
                .rows(maps)
                .size((long) maps.size());
    }

    private static void handleFetchStore(RunContext runContext, ResultSet rs, ColumnDefinitions columnDefinitions, AstraOutput.AstraOutputBuilder outputBuilder) throws IOException {
        File tempFile = runContext.tempFile(".ion").toFile();
        BufferedWriter fileWriter = new BufferedWriter(new FileWriter(tempFile));
        AtomicLong count = new AtomicLong();
        try (OutputStream outputStream = new FileOutputStream(tempFile)) {
            rs.forEach(throwConsumer(row -> {
                count.getAndIncrement();
                FileSerde.write(outputStream, AstraHelper.convertRow(row, columnDefinitions));
            }));
        }

        fileWriter.flush();
        fileWriter.close();

        outputBuilder
                .uri(runContext.putTempFile(tempFile))
                .size(count.get());
    }

    private static void handleFetchOne(ResultSet rs, ColumnDefinitions columnDefinitions, AstraOutput.AstraOutputBuilder outputBuilder) {
        outputBuilder
                .row(AstraHelper.convertRow(rs.one(), columnDefinitions))
                .size(1L);
    }


}
