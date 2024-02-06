import dataclasses
from collections import defaultdict
from typing import Mapping, Any, List, Callable


class ValueAccess:
    def __init__(self, *keys, ignore_case=True):
        self.keys = keys
        self.ignore_case = ignore_case

    def __call__(self, datum) -> tuple:
        values = []
        for keyname in self.keys:
            if hasattr(datum, keyname):
                value = getattr(datum, keyname)
            else:
                try:
                    value = datum[keyname]
                except KeyError:
                    raise ValueAccessError(f"{datum} has No such key or attribute {keyname}")
            values.append(value)

        retrieved = tuple(values)
        return retrieved


class ValueAccessError(Exception):
    pass


def value(datum):
    return 0


def select(
    data: List[Any],
    filter_with: Callable[[Any], List[Any]] = None,
    key_extraction_function: Callable[[Any], Any] = value,
    aggregate_function: Callable[[List[Any]], Any] = None,
) -> List:

    if filter_with:
        data = [datum for datum in data if filter_with(datum)]

    if aggregate_function:
        data = group_and_then_aggregate(
            data,
            value_extraction_function=key_extraction_function,
            aggregate_function=aggregate_function,
        )

    return data


def group_and_then_aggregate(
        data,
        value_extraction_function: Any = None,
        aggregate_function: Callable[[List[Any]], Any] = None,
) -> List[Mapping]:

    value_extraction_function = UnifiedValueExtraction(value_extraction_function)
    aggregate_function = UnifiedAggregation(aggregate_function)
    grouped_data = defaultdict(list)

    for datum in data:
        keys = value_extraction_function(datum)
        grouped_data[keys].append(datum)

    aggregations = []
    for group_key in grouped_data:
        data_in_same_group = grouped_data[group_key]
        new_row = dict(zip(value_extraction_function.keys, group_key))
        new_row[repr(aggregate_function)] = aggregate_function(data_in_same_group)
        aggregations.append(new_row)

    return aggregations


class UnifiedAggregation:
    def __init__(self, aggregate_function):
        self.aggregate_function = aggregate_function

    def __repr__(self):
        try:
            return f"{self.aggregate_function.__name__}"
        except AttributeError:
            return repr(self.aggregate_function)

    def __call__(self, *args, **kwargs):
        return self.aggregate_function(*args, **kwargs)


def is_lambda(possible_function):
    return callable(possible_function) and possible_function.__name__ == "<lambda>"


class UnifiedValueExtraction:
    def __init__(self, value_extraction_function):
        self.value_extraction_function = value_extraction_function
        if hasattr(value_extraction_function, "keys"):
            self.keys = value_extraction_function.keys
        elif is_lambda(value_extraction_function):
            self.keys = ('value',)
        else:
            try:
                self.keys = (value_extraction_function.__name__,)
            except AttributeError:
                self.keys = repr(value_extraction_function)

    def __call__(self, *args, **kwargs):
        extracted_value = self.value_extraction_function(*args, **kwargs)
        if not isinstance(extracted_value, tuple):
            extracted_value = (extracted_value,)
        return extracted_value


def odd_or_even(number) -> str:
    if number % 2 == 0:
        return "even"
    return "odd"


class LessThan:
    def __init__(self, criteria):
        self.criteria = criteria

    def __call__(self, datum):
        return datum <= self.criteria


class MostPopularValue:
    def __init__(self, *keys):
        self.keys = keys
        if not keys:
            self.value_access = lambda x: x
        else:
            self.value_access = ValueAccess(*keys)

    def __repr__(self):
        suffix = ""
        if self.keys:
            if len(self.keys) == 1:
                suffix = f":{self.keys[0]}"
            else:
                suffix = f":{self.keys}"
        return f"MostPopular{suffix}"

    def __call__(self, data):
        # https://zh.wikipedia.org/zh-tw/众数_(数学)
        count_dict = defaultdict(int)
        for datum in data:
            values = self.value_access(datum)
            count_dict[values] += 1
        return max(count_dict)
