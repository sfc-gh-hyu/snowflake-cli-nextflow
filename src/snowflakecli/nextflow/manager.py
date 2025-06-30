from snowflake.cli.api.sql_execution import SqlExecutionMixin
from snowflake.connector.cursor import SnowflakeCursor
from snowflakecli.nextflow.util.cmd_runner import CommandRunner
from snowflakecli.nextflow.service_spec import Specification, Spec, Container, parse_stage_mounts, VolumeConfig, VolumeMount, Volume
from dataclasses import dataclass
from snowflake.cli.api.exceptions import CliError
from snowflake.cli.api.console import cli_console as cc
import os
import tarfile
import tempfile
from pathlib import Path
import random
import string
from datetime import datetime
from snowflake.cli._plugins.spcs.common import (
    new_logs_only,
    filter_log_timestamp,
)
from typing import List
import time
import json

@dataclass
class ProjectConfig:
    computePool: str = ""
    workDirStage: str = ""
    volumeConfig: VolumeConfig = None

class NextflowManager(SqlExecutionMixin):

    def __init__(self, project_dir: str, profile: str = None):
        super().__init__()
        self._project_dir = Path(project_dir)
        if not self._project_dir.exists() or not self._project_dir.is_dir():
            raise CliError(f"Invalid project directory '{project_dir}'")

        self._profile = profile
        
        # Generate random alphanumeric runtime ID using UTC timestamp and random seed
        utc_timestamp = int(datetime.now().timestamp())
        random.seed(utc_timestamp)
        
        # Generate 8-character runtime ID that complies with Nextflow naming requirements
        # Must start with lowercase letter, followed by lowercase letters and digits
        first_char = random.choice(string.ascii_lowercase)
        remaining_chars = ''.join(random.choices(string.ascii_lowercase + string.digits, k=7))
        self._run_id = first_char + remaining_chars
        self.service_name = f"NXF_MAIN_{self._run_id}"

    def _parse_config(self) -> ProjectConfig:
        """
        Parse the nextflow.config file and return a ProjectConfig object.
        """

        config = ProjectConfig()

        def parse_config_line(line: str) -> None:
            key, val = line.split(" = ")
            if key == "snowflake.computePool":
                config.computePool = val.strip().replace("'", "")
            elif key == "snowflake.stageMounts":
                config.volumeConfig = parse_stage_mounts(val.strip().replace("'", ""))
            elif key == "snowflake.workDirStage":
                config.workDirStage = val.strip().replace("'", "")

        stderr = []
        def collect_stderr(line: str) -> None:
            stderr.append(line)

        runner = CommandRunner()
        runner.set_stdout_callback(parse_config_line)
        runner.set_stderr_callback(collect_stderr)
        cmds = ["nextflow", "config", self._project_dir.name, "-flat"]
        if self._profile:
            cmds+=["-profile", self._profile]

        ret = runner.run(cmds)
        if ret != 0:
            err_msg = "Failed to parse nextflow.config\n"
            err_msg += "\n".join(stderr)
            raise CliError(err_msg)

        
        return config

    def _upload_project(self, config: ProjectConfig) -> str:
        """
        Create a tarball of the project directory and upload to Snowflake stage.
        """
        
        # Create temporary file for the tarball
        with tempfile.NamedTemporaryFile(suffix='.tar.gz', delete=False) as temp_file:
            temp_tarball_path = temp_file.name
        
        try:
            cc.step("Creating tarball...")
            # Create tarball excluding .git directory
            self._create_tarball(self._project_dir, temp_tarball_path)
            
            cc.step(f"Uploading to stage {config.workDirStage}...")
            # Upload to Snowflake stage
            self.execute_query(f"PUT file://{temp_tarball_path} {config.workDirStage}/{self._run_id}")

            return temp_tarball_path
            
        finally:
            # Clean up temporary file
            if os.path.exists(temp_tarball_path):
                os.unlink(temp_tarball_path)
    
    def _create_tarball(self, project_path: Path, tarball_path: str):
        """
        Create a tarball of the project directory, excluding .git and other unwanted files.
        
        Args:
            project_path: Path to the project directory
            tarball_path: Path where the tarball should be created
        """
        
        def tar_filter(tarinfo):
            """Filter function to exclude unwanted files/directories"""
            # Exclude other common unwanted files/directories
            excluded_patterns = [
                '.git',
                '.gitignore',
            ]
            
            for pattern in excluded_patterns:
                if pattern in tarinfo.name:
                    return None
            
            return tarinfo
        
        try:
            with tarfile.open(tarball_path, 'w:gz') as tar:
                # Add all files from project directory with filtering
                tar.add(
                    project_path, 
                    arcname=project_path.name,  # Use project name as root in archive
                    filter=tar_filter
                )
                
        except Exception as e:
            raise CliError(f"Failed to create tarball: {str(e)}")
        
    def _run_nextflow(self, config: ProjectConfig, tarball_path: str):
        """
        Run the nextflow pipeline.
        """
        tags = json.dumps({
            "NEXTFLOW_JOB_TYPE": "main",
            "NEXTFLOW_RUN_ID": self._run_id,
        })

        self.execute_query(f"alter session set query_tag = '{tags}'")

        workDir = "/mnt/workdir"
        tarball_filename = os.path.basename(tarball_path)

        run_script = f"""
        mkdir -p /mnt/project
        cd /mnt/project
        tar -zxf {workDir}/{tarball_filename}
        nextflow run . -name {self._run_id} -ansi-log true -profile {self._profile} -work-dir /mnt/workdir -with-report /mnt/workdir/report.html -with-trace /mnt/workdir/trace.txt -with-timeline /mnt/workdir/timeline.html
        """

        config.volumeConfig.volumeMounts.append(VolumeMount(name="workdir", mountPath=workDir))
        config.volumeConfig.volumes.append(Volume(name="workdir", source=config.workDirStage+"/"+self._run_id+"/"))

        spec = Specification(
            spec = Spec(
                containers = [
                    Container(
                        name="nf-main",
                        image="/HYU/PUBLIC/NF_REPO/nf-snowflake:1.0",
                        command=["/bin/bash", "-c", run_script],
                        volumeMounts=config.volumeConfig.volumeMounts
                    )
                ],
                volumes = config.volumeConfig.volumes
            )
        )
        
        # Get YAML string for inline spec
        yaml_spec = spec.to_yaml()

        execute_sql = f"""
EXECUTE JOB SERVICE
IN COMPUTE POOL {config.computePool}
NAME = '{self.service_name}'
FROM SPECIFICATION $$
{yaml_spec}
$$
        """
        self.execute_query(execute_sql)
        self.execute_query("alter session unset query_tag")

    def run(self) -> SnowflakeCursor:
        cc.step("Parsing nextflow.config...")
        config = self._parse_config()

        tarball_path = None
        with cc.phase("Uploading project..."):
            tarball_path = self._upload_project(config)
        
        cc.step("Submitting nextflow job...")
        self._run_nextflow(config, tarball_path)

    def logs(
        self,
        service_name: str,
        instance_id: str,
        container_name: str,
        num_lines: int,
        previous_logs: bool = False,
        since_timestamp: str = "",
        include_timestamps: bool = False,
    ):
        cursor = self.execute_query(
            f"call SYSTEM$GET_SERVICE_LOGS('{service_name}', '{instance_id}', '{container_name}', "
            f"{num_lines}, {previous_logs}, '{since_timestamp}', {include_timestamps});"
        )

        for log in cursor.fetchall():
            yield log[0] if isinstance(log, tuple) else log

    def stream_logs(
        self,
        service_name: str,
        instance_id: str,
        container_name: str,
        num_lines: int,
        since_timestamp: str,
        include_timestamps: bool,
        interval_seconds: int,
    ):
        try:
            prev_timestamp = since_timestamp
            prev_log_records: List[str] = []

            while True:
                raw_log_blocks = [
                    log
                    for log in self.logs(
                        service_name=service_name,
                        instance_id=instance_id,
                        container_name=container_name,
                        num_lines=num_lines,
                        since_timestamp=prev_timestamp,
                        include_timestamps=True,
                    )
                ]

                new_log_records = []
                for block in raw_log_blocks:
                    new_log_records.extend(block.split("\n"))

                new_log_records = [line for line in new_log_records if line.strip()]

                if new_log_records:
                    dedup_log_records = new_logs_only(prev_log_records, new_log_records)
                    for log in dedup_log_records:
                        yield filter_log_timestamp(log, include_timestamps)

                    if len(dedup_log_records) > 0:
                        prev_timestamp = dedup_log_records[-1].split(" ", 1)[0]
                        prev_log_records = dedup_log_records

                time.sleep(interval_seconds)

        except KeyboardInterrupt:
            return

        
        
        
