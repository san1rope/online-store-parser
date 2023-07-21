from dataclasses import dataclass
from environs import Env


@dataclass
class OperatingTime:
    request_delay: float
    req_retry_delay: float
    once_per: float


@dataclass
class Miscellaneous:
    proxy: str
    processes: int


@dataclass
class Config:
    operate_time: OperatingTime
    misc: Miscellaneous


async def load_config(path: str = None):  # Получаю все данные с .env и записываю в глобальные переменные
    env = Env()
    env.read_env(path)

    return Config(
        operate_time=OperatingTime(
            request_delay=env.float("REQUEST_DELAY"),
            req_retry_delay=env.float("REQUEST_RETRY_DELAY"),
            once_per=env.float("ONCE_PER")
        ),
        misc=Miscellaneous(
            proxy=env.str("TABLE_PROXY"),
            processes=env.int("PROCESSES")
        )
    )
