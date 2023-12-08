process COBS_QUERY {
    tag "$meta.id"
    label 'process_low'

    conda "${moduleDir}/environment.yml"
    container "${workflow.containerEngine == 'singularity' && !task.ext.singularity_pull_docker_container ?
        'https://depot.galaxyproject.org/singularity/cobs:0.3.0--hdcf5f25_0' :
        'biocontainers/cobs:0.3.0--hdcf5f25_0'}"

    input:
    tuple val(meta),  path(query)
    tuple val(meta_index), path(index)

    output:
    tuple val(meta), path("matches.gz"), emit: matches
    path "versions.yml"                , emit: versions

    when:
    task.ext.when == null || task.ext.when

    script:
    // Note: xz is explicitly supported in this module as COBS indexes compress well under xz (see https://doi.org/10.1101/2023.04.15.536996)

    // variables setup
    def args = task.ext.args ?: ''
    def should_load_the_whole_index_into_RAM = args.contains("--load-complete")
    def index_is_gzip_compressed = index.toString().endsWith(".gz")
    def index_is_xz_compressed = index.toString().endsWith(".xz")
    def decompress_tool = ""
    def get_index_size_command = ""

    if (index_is_gzip_compressed) {
        decompress_tool = "gzip"
        get_index_size_command = "index_size=\$(gzip --list "$index" | tail -n1 | awk '{print \\\$2}')"
    } else if (index_is_xz_compressed) {
        decompress_tool = "xz"
        get_index_size_command = "index_size=\$(xz --list --robot "$index" | grep file | awk '{print \\\$5}')"
    }
    def command =
        """
            set -euo pipefail
        """

    // run COBS
    if (index_is_gzip_compressed || index_is_xz_compressed) {
        if (should_load_the_whole_index_into_RAM) {
            // streams compressed index to COBS
            command += get_index_size_command
            command += """
                            cobs \\
                                query \\
                                $args \\
                                -T $task.cpus \\
                                -i <(${decompress_tool} --decompress --stdout "${index}") \\
                                -f <(zcat $query) \\
                                --index-sizes \$index_size \\
                       """
        } else {
            // decompresses compressed index to disk and mmap it
            def decompressed_index = index.toString().replaceAll(".{g,x}z\$", "")
            command += """
                            ${decompress_tool} --decompress --stdout "${index}" > "${decompressed_index}"

                            cobs \\
                                query \\
                                $args \\
                                -T $task.cpus \\
                                -i $decompressed_index \\
                                -f <(zcat $query) \\

                            rm "${decompressed_index}"
                       """
        }
    }else {
        // index is not compressed - will be loaded completely into RAM (if --load-complete is passed) or memory mapped otherwise
        command += """
                        cobs \\
                            query \\
                            $args \\
                            -T $task.cpus \\
                            -i $index \\
                            -f <(zcat $query) \\
                   """
    }
    command +=
        """ | gzip > matches.gz

                cat <<-END_VERSIONS > versions.yml
                "${task.process}":
                    cobs: \$(cobs version 2>&1 | awk '{print \$3}')
                END_VERSIONS
        """
    command
}
