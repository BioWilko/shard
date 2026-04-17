import pytest
from shard.nfconfig import NextflowConfigParser


@pytest.fixture
def parser():
    return NextflowConfigParser()


class TestNextflowConfigParser:
    def test_simple_manifest_block(self, parser):
        text = """
manifest {
    name = 'my-workflow'
    version = '1.0.0'
    description = 'A test workflow'
}
"""
        blocks = parser.parse(text)
        assert blocks["manifest"]["name"] == "my-workflow"
        assert blocks["manifest"]["version"] == "1.0.0"
        assert blocks["manifest"]["description"] == "A test workflow"

    def test_double_quoted_values(self, parser):
        text = """
manifest {
    name = "double-quoted"
}
"""
        blocks = parser.parse(text)
        assert blocks["manifest"]["name"] == "double-quoted"

    def test_line_comments_stripped(self, parser):
        text = """
manifest {
    name = 'workflow' // this is a comment
    version = '2.0.0'
}
"""
        blocks = parser.parse(text)
        assert blocks["manifest"]["name"] == "workflow"
        assert blocks["manifest"]["version"] == "2.0.0"

    def test_block_comments_stripped(self, parser):
        text = """
/* global comment */
manifest {
    name = 'workflow'
}
"""
        blocks = parser.parse(text)
        assert blocks["manifest"]["name"] == "workflow"

    def test_url_in_value_not_stripped(self, parser):
        text = """
params {
    base = 'https://example.com/data'
}
"""
        blocks = parser.parse(text)
        assert blocks["params"]["base"] == "https://example.com/data"

    def test_multiple_blocks(self, parser):
        text = """
manifest {
    name = 'wf'
}
params {
    ref = 'hg38'
}
"""
        blocks = parser.parse(text)
        assert "manifest" in blocks
        assert "params" in blocks
        assert blocks["params"]["ref"] == "hg38"

    def test_nested_with_name_block(self, parser):
        text = """
process {
    withName: 'ALIGN' {
        container = 'bwa:0.7'
    }
    executor = 'slurm'
}
"""
        blocks = parser.parse(text)
        assert blocks["process"]["executor"] == "slurm"
        assert blocks["process"]["withName:ALIGN"]["container"] == "bwa:0.7"

    def test_nested_with_label_block(self, parser):
        text = """
process {
    withLabel: 'heavy' {
        container = 'bigimage:1.0'
    }
}
"""
        blocks = parser.parse(text)
        assert blocks["process"]["withLabel:heavy"]["container"] == "bigimage:1.0"

    def test_nested_with_name_double_quoted(self, parser):
        text = """
process {
    withName: "TRIM_READS" {
        container = 'trimmomatic:0.39'
    }
}
"""
        blocks = parser.parse(text)
        assert blocks["process"]["withName:TRIM_READS"]["container"] == "trimmomatic:0.39"

    def test_get_all_collects_from_nested_blocks(self, parser):
        text = """
process {
    withName: 'BWA' {
        container = 'bwa:0.7.17'
    }
    withName: 'SAMTOOLS' {
        container = 'samtools:1.18'
    }
}
"""
        blocks = parser.parse(text)
        containers = NextflowConfigParser.get_all(blocks, "container")
        assert set(containers) == {"bwa:0.7.17", "samtools:1.18"}

    def test_get_all_ignores_non_string_values(self, parser):
        text = """
process {
    withName: 'A' {
        container = 'image:1.0'
    }
}
"""
        blocks = parser.parse(text)
        containers = NextflowConfigParser.get_all(blocks, "container")
        assert containers == ["image:1.0"]

    def test_empty_block(self, parser):
        text = """
manifest {
}
"""
        blocks = parser.parse(text)
        assert blocks["manifest"] == {}

    def test_no_blocks(self, parser):
        blocks = parser.parse("// just a comment\n")
        assert blocks == {}

    def test_multiline_block_comment(self, parser):
        text = """
/*
 * Multi-line
 * block comment
 */
manifest {
    name = 'wf'
}
"""
        blocks = parser.parse(text)
        assert blocks["manifest"]["name"] == "wf"

    def test_params_block(self, parser):
        text = """
params {
    container_tag = '1.2.3'
    genome = 'hg38'
}
"""
        blocks = parser.parse(text)
        assert blocks["params"]["container_tag"] == "1.2.3"
        assert blocks["params"]["genome"] == "hg38"

    def test_top_level_dotted_assignment(self, parser):
        text = "docker.registry = 'quay.io'\n"
        blocks = parser.parse(text)
        assert blocks["docker"]["registry"] == "quay.io"

    def test_multiple_top_level_dotted_assignments(self, parser):
        text = (
            "docker.registry   = 'quay.io'\n"
            "docker.enabled    = 'true'\n"
            "apptainer.registry = 'quay.io'\n"
        )
        blocks = parser.parse(text)
        assert blocks["docker"]["registry"] == "quay.io"
        assert blocks["docker"]["enabled"] == "true"
        assert blocks["apptainer"]["registry"] == "quay.io"

    def test_dotted_assignment_merged_with_block(self, parser):
        text = (
            "docker.registry = 'quay.io'\n"
            "docker {\n"
            "    enabled = 'true'\n"
            "}\n"
        )
        blocks = parser.parse(text)
        assert blocks["docker"]["registry"] == "quay.io"
        assert blocks["docker"]["enabled"] == "true"
