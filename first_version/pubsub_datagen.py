import random
import argparse
import numpy as np
from datetime import datetime

from collections import Counter

from dataclasses import dataclass
from dataclasses import field

from multiprocessing import Lock as MpLock
from threading import Lock as ThLock

from typing import Iterator
from typing import Literal
from typing import Union
from typing import Tuple, List



from pyflink.common import WatermarkStrategy, Encoder, Types
from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode
from pyflink.datastream.connectors.file_system import FileSource, StreamFormat, FileSink, OutputFileConfig, RollingPolicy

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
    "temp": (-25, 71),                      # tuple[float, float]
    "wind": (0, 100),                       # tuple[float, float]
    "date": ("2020-01-01", "2023-04-01"),   # tuple[str, str]
    "city": CITIES,
    "wind_direction": DIRECTIONS,
}


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


@dataclass
class Publication:
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
        
    ) -> "Publication":
        return cls(
            station_id=gen_value("station_id"),
            city=gen_value("city"),
            temp=gen_value("temp", temp_range),
            wind=gen_value("wind", wind_range),
            wind_direction=gen_value("wind_direction"),
            date=gen_value("date", date_range),
        )

    @classmethod
    def generate_n(
        cls,
        n: int,
        *args,
        **kwargs,
    ) -> Iterator["Publication"]:
        for i in range(n):
            yield cls.generate_one(*args, **kwargs)


print(list(Publication.generate_n(3, temp_range=(100, 200))))
list(Publication.generate_n(3, wind_range=(300, 303)))

class SubGen:
    """
    Subscriptions generator.
    After instatiation -> use `.gen_n_with_param()` method as entry point.
    """
    
    def __init__(
        self,
        pub_cls = Publication,
        parallel_type: Literal["threading", "multiproc"] = "threading",
        # max_counts: dict[tuple[str, ...], int] = None,
    ) -> None:
        self.pub_cls = pub_cls
        self.fields = {k: v.type for k, v in pub_cls.__dataclass_fields__.items()}
        self.lock = MpLock() if not parallel_type.startswith("thr") else ThLock()
        # --- Stats ---
        # These variables are set inside `gen_n_with_param()` method
        self.counts = Counter()
        self.max_counts = {(k, ): -1 for k in self.fields.keys()}   # | max_counts
        self.n = 0

    def _gen_filter_for_field(
        self,
        field_name: str,
        filter_val: FilterValT = None,
        filter_val_range: FilterValRangeT = None,
        preset_op: OpT = None,
        not_this_op: OpT = None,
    ) -> FieldFilterT:
        ftype = self.fields[field_name]
        available_ops = ["=", "!="]
        if ftype in (float, int, datetime.date, datetime):
            available_ops.extend(("<=", ">=", "<", ">"))
        if not_this_op:
            available_ops.remove(not_this_op)
        # print("Present operator?: ", preset_op)
        if preset_op and preset_op in available_ops:
            op = preset_op
        else:
            op = random.choice(available_ops)
        filter_val = filter_val or gen_value(field_name, filter_val_range)
        return field_name, op, filter_val

    
    def gen_with_fields(self, *fields) -> Tuple[List[FieldFilterT], ...]:
        if not fields:
            # All fields
            fields = self.fields.keys()
        filters = []
        
        for field_name in fields:
            assert field_name in self.fields, f"{field_name} not in set of filelds: {self.fields}"
    
            with self.lock:
                for original_key, value in self.max_counts.items():
                    appended = False
                    if original_key[0] == field_name: 
                        if len(original_key) >= 2:
                            continue
                        
                        # verificam daca in max_counts avem un pereche cu operator si cu acelasi field_name
                        new_max_counts =  dict(self.max_counts)
                        del new_max_counts[original_key]

                        for another_key in new_max_counts:
                            if len(another_key) == 2 and another_key[0] == field_name:
                                # daca am gasit atunci vom indeplini prima data criteriile pentru aceasta
                                if self.counts[another_key] < self.max_counts[another_key]:
                                    filters.append(self._gen_filter_for_field(field_name, preset_op=another_key[1]))
                                    self.counts[another_key] += 1
                                    self.counts[(field_name, )] += 1
                                    appended = True
                                else:
                                    filters.append(self._gen_filter_for_field(field_name, not_this_op=another_key[1]))
                                    self.counts[(field_name, )] += 1
                                    appended = True
                        if len(original_key) < 2 and appended == False:
                            # pentru restul vom adauga un operator random
                            filters.append(self._gen_filter_for_field(field_name))
                            self.counts[(field_name, )] += 1  
        return filters
    


    def gen_n_with_param(self, n: int, key_percentages: dict[tuple[str, ...], float] = None, filter_percentages:dict[tuple[str, str], float] = None):
        """
        >>> sg = SubGen()
        >>> n = 100           # number of subscriptions
        >>> key_percentages = {    # configure field occurrence ratios. Field not mentioned --> 100%
                ("city", ): 0.11,
                ("temp", ): 0.71,
            }

        >>> subscriptions = [sub for sub in sg.gen_n_with_param(n, key_percentages=key_percentages)]:
        >>> sg.print_counts()    # check actual counts vs maximum counts (as per ratio)
        """
        self.n = n
        key_percentages = key_percentages or {}
        filter_percentages = filter_percentages or {}
        fields = self.fields.keys()
        # Reset counter & max_counts
        with self.lock:
            self.counts = Counter()                        # Keep track of counts for each key
            self.max_counts = {(k, ): n for k in fields}   # Maximum number times a key can occur
            fields_ratios = {k: 1. for k in fields}        # Occurrence ratio by default 100% for all fields
            for key, ratio in key_percentages.items():
                self.max_counts[key] = int(n * ratio)

                if key in fields_ratios:
                    fields_ratios[key] = ratio
                # pentru aceasta key am un numar total de aparitii dintre care avem un numar de aparitii cu un operator
                for key_filter, ratio_filter in filter_percentages.items():
                    if key[0] == key_filter[0]:
                        # in cazul asta ar trebui sa calculam rata pentru operator
                        self.max_counts[key_filter] = int(self.max_counts[key] * ratio_filter)
                        self.max_counts[key] = int(n * ratio) - int(self.max_counts[key] * ratio_filter)
                
            ratios = np.array([fields_ratios[key] for key in fields], dtype="float16")

        
        print("Self max:", self.max_counts)

        # print("Field generation ratios:", fields_ratios, "\n")

        for i in range(n):
            selected_fields = []
            with self.lock:
                rand_sel = np.random.random(ratios.shape[0]) <= ratios   # boolean if
                for fname, is_selected in zip(fields, rand_sel):
                    fname_max_counts = 0;
                    for key, value in self.max_counts.items():
                        if key[0] == fname:
                            fname_max_counts += value
            
                    if is_selected and self.counts[(fname, )] < fname_max_counts:
                        selected_fields.append(fname)
            yield self.gen_with_fields(*selected_fields)

    def print_counts(self):       
        print("-" * 100)
        print("Field name".ljust(20), "Counts".ljust(10), "Operator =".ljust(20), "Percentage".ljust(20), "Matches max")
        print("-" * 100)
        with self.lock:
            for fname in self.fields.keys():
                fname_max_counts = 0;
                for key, value in self.max_counts.items():
                    if key[0] == fname:
                        fname_max_counts += value
                        
                print(
                    fname.ljust(20),
                    str(self.counts[(fname,)]).ljust(10),
                    str(self.counts[(fname, "=")]).ljust(20),
                    str(round(self.counts[(fname,)] / n, 2) * 100).ljust(20),
                    str(self.counts[(fname,)] == fname_max_counts)
                )
        print("-" * 100)


def parallel_subscription_gen(subs_data_generator, output_path=None, parallelism: int = 10):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_runtime_mode(RuntimeExecutionMode.BATCH)
    # write all the data to one file

    env.set_parallelism(parallelism)

    # define the source
    print("Executing Subscription Generator with parallelism:", parallelism)
    print("Use --input to specify file input.")
    ds = env.from_collection(subs_data_generator)

    # Map everything to string
    ds = ds.map(
        lambda subscription: [tuple(str(x)for x in tup) for tup in subscription],
        output_type=Types.LIST(Types.TUPLE([Types.STRING(), Types.STRING(), Types.STRING()]))
    )

    # define the sink
    if output_path is not None:
        ds.sink_to(
            sink=FileSink.for_row_format(
                base_path=output_path,
                encoder=Encoder.simple_string_encoder())
            .with_output_file_config(
                OutputFileConfig.builder()
                .with_part_prefix("prefix")
                .with_part_suffix(".ext")
                .build())
            .with_rolling_policy(RollingPolicy.default_rolling_policy())
            .build()
        )
    else:
        print("Printing result to stdout. Use --output to specify output path.")
        ds.print()
        print("---- DONE ----")

    # submit for execution
    env.execute()

if __name__ == "__main__":
    # Create a new ArgumentParser object
    parser = argparse.ArgumentParser(description='How this program should work?')

    parser.add_argument('-n', '--number', type=int, help='Number of messages')
    parser.add_argument('-t', '--type', type=str, help='Do you prefer using iteration or parallelism?')

    args = parser.parse_args()

    n = args.number
    type = args.type

    sg = SubGen()

    results = []
    count = Counter()

    key_percentages = {
        ("city", ): 0.71,
        ("temp", ): 0.8,
    }
    filter_percentages = {
        ("city", "="): 0.5,
    }

    if type == 'iteration':
        start_time = datetime.now()
        # do your work here
        for res in sg.gen_n_with_param(n, key_percentages=key_percentages, filter_percentages=filter_percentages):
            results.append(res)
        sg.print_counts()
        # print(results)

        end_time = datetime.now()
        print('Duration: {}'.format(end_time - start_time))

    elif type == 'parallelism':
        # do your work here
        subs_data_generator = sg.gen_n_with_param(n, key_percentages=key_percentages, filter_percentages=filter_percentages)

        start_time = datetime.now()
        parallel_subscription_gen(subs_data_generator, "output", parallelism=5)
        sg.print_counts()

        end_time = datetime.now()
        print('Duration: {}'.format(end_time - start_time))
