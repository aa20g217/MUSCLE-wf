"""
A wf for Multiple Sequence Alignment using MUSCLE (MUltiple Sequence Comparison by Log- Expectation).
"""


import subprocess
from pathlib import Path

from flytekit import LaunchPlan, workflow
from latch.types import LatchDir,LatchFile
from latch import large_task
from latch.resources.launch_plan import LaunchPlan
import os,shutil
from typing import Optional, Annotated
from flytekit.core.annotation import FlyteAnnotation

@large_task
def runwf(    fasta_file: Optional[LatchFile],
    aa_sequence: Optional[str],output_dir: LatchDir) -> LatchDir:
    os.mkdir('results')
    os.chdir("/root/results/")
    if fasta_file is not None:

        subprocess.run(
            [
                "muscle",
                "-in",
                fasta_file.local_path,
                "-fastaout",
                "alignment.afa",
                "-clwout","alignment.aln",
                	"-htmlout", "alignment.html"
                #"-log","muscle.log"
            ]
        )
    else:
        text_file = open("input.txt", "w")
        text_file.write(aa_sequence)
        text_file.close()

        subprocess.run(
            [
                "muscle",
                "-in",
                "input.txt",
                "-fastaout",
                "alignment.afa",
                "-clwout","alignment.aln",
                	"-htmlout", "alignment.html"
                #"-log","muscle.log"
            ]
        )


    print(os.listdir("/root/"))


    local_output_dir = str(Path("/root/results/").resolve())

    remote_path=output_dir.remote_path
    if remote_path[-1] != "/":
        remote_path += "/"

    return LatchDir(local_output_dir,remote_path)

#-> LatchDir
@workflow
def MUSCLE(output_dir: LatchDir,input_sequence_fork: str = "text",
        fasta_file: Optional[
        Annotated[
            LatchFile,
            FlyteAnnotation(
                {
                    "rules": [
                        {
                            "regex": "(.fasta|.fa|.faa|.txt|.fas)$",
                            "message": "Only .fasta, .fa, .fas,.txt, or .faa extensions are valid",
                        }
                    ],
                }
            ),
        ]
    ] = None,
    aa_sequence: Optional[
        Annotated[
            str,
            FlyteAnnotation(
                {
                    "appearance": {
                        "type": "paragraph",
                        "placeholder": ">SequenceOne\nLESPNCDWKNNR...\n>SequenceTwo\nRLENKNNCSPDW...\n>SequenceThree\nCDWKNNENPDEA...",
                    },
                    "rules": [
                        {
                            "message": "Paste a set of sequences in fasta format. The name line must start with `>`.",
                        }
                    ],
                }
            ),
        ]
    ] = None):
    """

    A wf for Multiple Sequence Alignment using MUSCLE (MUltiple Sequence Comparison by Log- Expectation).
    ----

    `MUSCLE (v5):` A program to create multiple sequence alignments of a large number of sequences. Prominent features are rapid sequence distance computation using k-mer counting, a profile function computing a log-expectation scores, and tree-dependent partitioning of the sequences.
    __metadata__:
        display_name: MUSCLE (MUltiple Sequence Comparison by Log- Expectation)
        author:
            name: Akshay
            email: akshaysuhag2511@gmail.com
            github:
        repository:
        license:
            id: MIT
        flow:
        - section: Fasta Sequences
          flow:
            - fork: input_sequence_fork
              flows:
                text:
                    display_name: Text
                    _tmp_unwrap_optionals:
                        - aa_sequence
                    flow:
                        - params:
                            - aa_sequence
                file:
                    display_name: File
                    _tmp_unwrap_optionals:
                        - fasta_file
                    flow:
                        - params:
                            - fasta_file

        - section: Output Settings
          flow:
          - params:
              - output_dir




    Args:

        fasta_file:
          Select input file. This file must be in FASTA format.

          __metadata__:
            display_name: Input File

        aa_sequence:
            Fasta sequences.

            __metadata__:
                display_name: Fasta Sequence(s)

        input_sequence_fork:

            __metadata__:
                display_name: Input Sequence

        output_dir:
          Where to save the results?.

          __metadata__:
            display_name: Output Directory
    """
    return runwf(fasta_file=fasta_file,aa_sequence=aa_sequence,output_dir=output_dir)

LaunchPlan(
    MUSCLE,
    "Test Data",
    {
        "fasta_file": LatchFile("s3://latch-public/test-data/4148/muscle/input.txt")
    },
)
