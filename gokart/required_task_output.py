from dataclasses import dataclass

@dataclass

class RequiredTaskOutput:
    task_name: str
    output_path: str

    def serialize(self) -> dict[str, str]:
        return {'task_name': self.task_name, 'output_path': self.output_path}
