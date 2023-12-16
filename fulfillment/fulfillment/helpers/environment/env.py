import os


def get_stage(stage: str = "alpha") -> str:
    return os.getenv("stage", stage).lower()


def is_prod() -> bool:
    return True if os.getenv("stage").lower() == "prod" else False


def get_region_name(region_name: str = "na") -> str:
    return os.getenv("region_name", region_name)


def get_region_id(region_id: int = 1) -> str:
    return os.getenv("region_id", region_id)
