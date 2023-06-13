import random
from datetime import datetime
import time

from collections import Counter
from collections import defaultdict
from functools import partial
from itertools import zip_longest

from dataclasses import dataclass
from dataclasses import field
from dataclasses import asdict

from typing import Iterator
from typing import Literal
from typing import Union


FilterValT = Union[str, float, int, datetime.date, datetime]
FilterValRangeT = Union[tuple[float, float], tuple[str, str], list[str]]
OpT = Literal["=", "!=", "<=", ">=", "<", ">"]
FieldFilterT = tuple[str, OpT, FilterValT]

CITIES = [
    "Iasi",
    "Bucuresti",
    "Timisoara",
    "Lyon",
    "Berne",
    "Basel",
    "Beyrouth",
    "Arusha",
]
DIRECTIONS = ["N", "NE", "NV", "S", "SE", "SV", "V", "E"]
DEFAULT_VAL_RANGES = {
    "station_id": (0, 100),                 # tuple[int, int]
    "temp": (-25, 25),                      # tuple[float, float]
    "wind": (10, 30),                       # tuple[float, float]
    "date": ("2023-06-01", "2023-06-05"),   # tuple[str, str]
    "city": CITIES,
    "wind_direction": DIRECTIONS,
}


# --- Workers ---

def gen_value(
    field_name: str,
    val_range: FilterValRangeT = None,
) -> FilterValT:
    """
    Value generator for given field. Can specify range for valur generation. Else uses defaults.
    >>> gen_value("wind_direction")
    >>> gen_value("city", val_range=["Iasi", "Basel"])
    """
    if not val_range:
        val_range = DEFAULT_VAL_RANGES[field_name]
    if field_name in ("station_id", ):
        return random.randrange(val_range[0], val_range[1])
    if field_name in ("temp", "wind", ):
        return random.randrange(val_range[0] * 20, val_range[1] * 20) / 20     # float
    if field_name in ("date", ):
        return datetime.fromtimestamp(
                random.randrange(
                    int(datetime.fromisoformat(val_range[0]).timestamp()),
                    int(datetime.fromisoformat(val_range[1]).timestamp()), ))
    if field_name in ("city", "wind_direction"):
        return random.choice(val_range)
    raise ValueError("Unknown field name?", field_name)


def split_stack(fields_stack, n):
    chunks = [fields_stack[i: i + n] for i in range(0, len(fields_stack), n)]
    return [list(filter(None, filt)) for filt in zip_longest(*chunks)]


def gen_subscriptions_worker(
    field_types,
    worker_param: "WorkerParam",
):
    """
    Given parameters, returns list of n subscriptions.
    """
    fields = field_types.keys()
    n = worker_param.n
    field_counts = worker_param.field_counts
    op_min_counts = worker_param.op_min_counts
    # [ [(fieldA, op), (fieldA, op)], [(fieldB, op), (fieldB, op)] ]
    fields_stack = []   # type: list[ list[tuple[str, str]] ]
    for field, field_count in field_counts.items():
        # Build operators list for current field
        field_type = field_types[field]
        available_ops = ["=", "!="]
        if field_type in (float, int, datetime.date, datetime):
            available_ops.extend(("<=", ">=", "<", ">"))
        operators = []
        op, op_count = op_min_counts[field]
        if op != "*":
            # At least "op_count" should be of given operator type
            operators.extend([op] * op_count)
        operators.extend(
            random.choice(available_ops) for _ in range(field_count - op_count)
        )
        assert len(operators) == field_count
        random.shuffle(operators)     # in-place!
        # Build field list
        field_occurrence = [field] * field_count
        assert len(field_occurrence) == len(operators)
        together = list(zip(field_occurrence, operators, (gen_value(field) for _ in range(field_count))))
        # together.extend([(None, None)] * (n - field_count))
        # assert len(together) == n
        random.shuffle(together)
        fields_stack.extend(together)
        # time.sleep(0.4)
    return split_stack(fields_stack, n)


# --- Models ---

@dataclass
class Publication:
    """
    >>> print(list(Publication.generate_n(3, temp_range=(100, 200))))
    >>> list(Publication.generate_n(3, wind_range=(300, 303)))

    """
    station_id: int
    city: str
    temp: float
    wind: float
    wind_direction: str
    date: datetime.date = field(default_factory=lambda: datetime.now().date)

    @classmethod
    def generate_one(
        cls,
        temp_range: tuple[float, float] = None,
        wind_range: tuple[float, float] = None,
        date_range: tuple[str, str] = None,
    ) -> list[tuple[str, FilterValT]]:
        return list(asdict(cls(
            station_id=gen_value("station_id"),
            city=gen_value("city"),
            temp=gen_value("temp", temp_range),
            wind=gen_value("wind", wind_range),
            wind_direction=gen_value("wind_direction"),
            date=gen_value("date", date_range),
        )).items())

    @classmethod
    def generate_n(
        cls,
        n: int,
        *args,
        **kwargs,
    ) -> Iterator[list[tuple[str, FilterValT]]]:
        for i in range(n):
            yield cls.generate_one(*args, **kwargs)


@dataclass
class WorkerParam:
    ix : int   # worker index
    n: int
    field_counts: dict[str, int]
    op_min_counts: dict[str, tuple[str, int]]


class SubGenSharder:
    """
    Subscriptions sharded generator: shards work per num of workers.
    """
    def __init__(
        self,
        pub_cls = Publication,
        # max_counts: dict[tuple[str, ...], int] = None,
    ) -> None:
        self._pub_cls = pub_cls
        self.fields = {k: v.type for k, v in pub_cls.__dataclass_fields__.items()}

    def shard(
        self,
        n: int,
        field_percentages: dict[str, float] = None,
        op_percentages: dict[str, tuple[str, float]] = None,
        num_workers: int = 1,
        # ---
        return_params_only: bool = False,
    ):
        """
        Shard work and set-up multiple workers.
        """
        field_percentages = field_percentages or {}
        op_percentages = op_percentages or {}
        fields = self.fields.keys()

        field_counts = {k: n for k in fields}
        op_min_counts = {k: ("*", 0) for k in fields}  # "(operator, min_counts)", "*" as placeholder

        for field, ratio in field_percentages.items():
            assert field in fields, f"{field=} not amoung {fields=}"
            field_counts[field] = int(round(n * ratio, 0))
        # Pentru keys unde avem un numar de aparitii cu un operator
        for (field, op), op_ratio in op_percentages.items():
            # in cazul asta ar trebui sa calculam rata pentru operator
            op_min_counts[field] = op, int(round(field_counts[field] * op_ratio, 0))

        counter = Counter()
        workers = []

        for i in range(1, num_workers + 1):
            worker_param = WorkerParam(
                ix=i,
                n=max(1, n // num_workers),    # n for worker
                field_counts=field_counts.copy(),
                op_min_counts=op_min_counts.copy(),
            )
            counter["n"] += worker_param.n
            if i == num_workers and counter["n"] < n:
                # Last worker: allocate remaining
                delta = n - counter["n"]
                worker_param.n += delta
                counter["n"] += delta
            for field, counts in field_counts.items():
                worker_param.field_counts[field] = max(1, counts // num_workers) if counts and counter[field] < counts else 0
                counter[field] += worker_param.field_counts[field]
                if i == num_workers and counter[field] < counts:
                    delta = counts - counter[field]
                    worker_param.field_counts[field] += delta
                # parametrize operator min counts ("=", ...)
                op, op_counts = op_min_counts[field]
                if op != "*":
                    ckey = f"{field}_{op}"
                    worker_param.op_min_counts[field] = (
                        op,
                        max(1, op_counts // num_workers) if op_counts and counter[ckey] < op_counts else 0
                    )
                    counter[ckey] += worker_param.op_min_counts[field][1]
                    if i == num_workers and counter[ckey] < op_counts:
                        delta = op_counts - counter[ckey]
                        worker_param.op_min_counts[field] = (
                            op,
                            worker_param.op_min_counts[field][1] + delta)
            workers.append(
                # Return only worker_param if return_params_only == True
                worker_param if return_params_only else
                # else, return a parametrized partial function (the worker) ready to execute
                partial(gen_subscriptions_worker, self.fields.copy(), worker_param)
            )
        return workers


def count_sub_results(res):
    counter = Counter()
    op_counter = defaultdict(Counter)
    for line in res:
        for field, op, val in line:
            counter[field] += 1
            op_counter[field][op] += 1
    op_counter = dict(op_counter)
    return counter, op_counter


def combine_sub_worker_results(results):
    counter = Counter()
    op_counter = defaultdict(Counter)
    for res in results:
        _counter, _op_counter = count_sub_results(res)
        counter.update(_counter)
        for key, op_counts in _op_counter.items():
            op_counter[key].update(op_counts)
    return counter, op_counter


def print_sub_counts(counter, op_counter, op = "="):       
    print("-" * 100)
    # (_, n), = counter.most_common(1)   # [('station_id', 10000)]   # (heuristic)
    print("Field name".ljust(20), "Counts".ljust(10), f"Op {op!r}".ljust(10), f"Op {op!r} percentage")
    print("-" * 100)
    for field, counts in sorted(counter.items()):

        print(
            field.ljust(20),
            str(counts).ljust(10),
            str(op_counter[field][op]).ljust(10),
            str(round(op_counter[field][op] / counts, 2) * 100).ljust(10),
        )
    print("-" * 100)


if __name__ == "__main__":

    from concurrent.futures import ProcessPoolExecutor
    from concurrent.futures import ThreadPoolExecutor

    import cpuinfo     # mamba install py-cpuinfo   OR   pip install py-cpuinfo

    print("Testing subscription generator.")
    print("Testing platform:")
    for key, value in cpuinfo.get_cpu_info().items():
        if key.startswith("cpuinfo"):
            continue
        print("\t{0}: {1}".format(key, value))

    n = 100_000
    num_workers = 5
    field_percentages = {
        "city": 0.2,
        "temp": 0.71,
    }
    op_percentages = {
        ("city", "="): 0.5,
    }
    sharder = SubGenSharder()

    print("Testing MULTIPROCESSING work on num workers:", num_workers)
    start_time = time.perf_counter()
    workers = sharder.shard(
        n,
        field_percentages,
        op_percentages,
        num_workers=num_workers,
        return_params_only=False, )
    with ProcessPoolExecutor(max_workers=min(5, num_workers)) as executor:
        futures = [executor.submit(
            # gen_subscriptions_worker, sharder.fields, worker   # if return_params_only is True
            worker
        ) for worker in workers]
        results = [future.result() for future in futures]   # results per worker

    total_time = time.perf_counter() - start_time
    print("Generated", n, "subscriptions over", num_workers, "workers")
    print('Duration: {}'.format(total_time))
    print("Rate of subscriptions per second:", round(n / max(1, total_time) / 1000, 3))
    print_sub_counts(*combine_sub_worker_results(results))   # PRINT COUNTS
    print(results[0][:2])
    print("-" * 100, end="\n\n")

    print("Testing MULTITHREADING work on num workers:", num_workers)
    start_time = time.perf_counter()
    workers = sharder.shard(
        n,
        field_percentages,
        op_percentages,
        num_workers=num_workers,
        return_params_only=False, )
    with ThreadPoolExecutor(max_workers=min(5, num_workers)) as executor:
        futures = [executor.submit(
            # gen_subscriptions_worker, sharder.fields, worker   # if return_params_only is True
            worker
        ) for worker in workers]
        results = [future.result() for future in futures]   # results per worker

    total_time = time.perf_counter() - start_time
    print("Generated", n, "subscriptions over", num_workers, "workers")
    print(f'Duration: {total_time}')
    print("Rate of subscriptions per second:", round(n / max(1, total_time) / 1000, 3))
    print_sub_counts(*combine_sub_worker_results(results))   # PRINT COUNTS
    print(results[0][:2])
