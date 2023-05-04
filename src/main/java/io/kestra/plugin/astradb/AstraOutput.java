package io.kestra.plugin.astradb;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Getter;

import java.net.URI;
import java.util.List;
import java.util.Map;

    /**
     * Input or Output can nested as you need
     */
    @Builder
    @Getter
    public class AstraOutput implements io.kestra.core.models.tasks.Output {
        @Schema(
                title = "Map containing the first row of fetched data",
                description = "Only populated if 'fetchOne' parameter is set to true."
        )
        private final Map<String, Object> row;

        @Schema(
                title = "Lit of map containing rows of fetched data",
                description = "Only populated if 'fetch' parameter is set to true."
        )
        private final List<Map<String, Object>> rows;

        @Schema(
                title = "The url of the result file on kestra storage (.ion file / Amazon Ion text format)",
                description = "Only populated if 'store' is set to true."
        )
        private final URI uri;

        @Schema(
                title = "The size of the fetched rows",
                description = "Only populated if 'store' or 'fetch' parameter is set to true."
        )
        private final Long size;

        @Schema(
                title = "The size of the binary response in bytes."
        )
        private final Integer bytes;
    }


