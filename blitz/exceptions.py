class BlitzError(Exception):
    """Base exception for all Blitz errors."""


class ParseError(BlitzError):
    """Error parsing a pipeline YAML file."""


class StepError(BlitzError):
    """Error executing a pipeline step."""

    def __init__(self, step_type: str, message: str):
        self.step_type = step_type
        super().__init__(f"[{step_type}] {message}")


class ExpressionError(BlitzError):
    """Error evaluating a filter/compute expression."""


class QualityGateError(StepError):
    """JIDOKA: Quality gate failed — pipeline stopped (Andon)."""

    def __init__(self, message: str):
        super().__init__("guard", message)


class AndonAlert(StepError):
    """JIDOKA: Anomaly detected — row count deviates from historical average."""

    def __init__(self, step_type: str, message: str):
        super().__init__(step_type, f"ANDON ALERT: {message}")
