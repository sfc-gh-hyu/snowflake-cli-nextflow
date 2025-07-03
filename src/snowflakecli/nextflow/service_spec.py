from dataclasses import dataclass, asdict
from snowflake.cli.api.exceptions import CliError
import yaml

@dataclass
class VolumeMount:
    name: str
    mountPath: str

@dataclass
class Container:
    name: str
    image: str
    command: list[str]
    volumeMounts: list[VolumeMount]

@dataclass
class Volume:
    name: str
    source: str

@dataclass
class Endpoint:
    name: str
    port: int
    public: bool

@dataclass
class Spec:
    containers: list[Container]
    volumes: list[Volume]
    endpoints: list[Endpoint]

@dataclass
class Specification:
    spec: Spec

    def to_yaml(self) -> str:
        """
        Convert ServiceSpec to YAML string.
        
        Returns:
            YAML string representation of the ServiceSpec
        """
        spec_dict = asdict(self)
        return yaml.dump(spec_dict, default_flow_style=False, indent=2)
    
@dataclass
class VolumeConfig:
    volumes: list[Volume]
    volumeMounts: list[VolumeMount]

def parse_stage_mounts(stage_mounts: str) ->VolumeConfig:
    volume_mounts = []
    volumes = []

    stage_mounts = stage_mounts.split(",")
    for index in range(len(stage_mounts)):
        mount = stage_mounts[index].split(":")
        if len(mount) != 2:
            raise CliError("Invalid stage mount expression: " + stage_mounts[index])

        volume_name = "vol-" + str(index+1)
        volumes.append(Volume(name=volume_name, source="@"+mount[0]))
        volume_mounts.append(VolumeMount(name=volume_name, mountPath=mount[1]))

    return VolumeConfig(volumes=volumes, volumeMounts=volume_mounts)






