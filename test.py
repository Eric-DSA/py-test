from dataclasses import dataclass

from aggregate_utility import odd_or_even, select, LessThan, ValueAccess, MostPopularValue


def test_filter_with_less_than():
    # 過濾少於 10 的數字
    data = list(range(30))

    filtered = select(
        data,
        filter_with=LessThan(10)
    )

    assert filtered == list(range(11))


def test_aggregate():
    # 想知道奇數跟偶數的數量
    data = list(range(30))
    aggregated = select(
        data,
        filter_with=LessThan(10),
        key_extraction_function=odd_or_even,
        aggregate_function=max,
    )

    assert aggregated == [
        {'odd_or_even': 'even', 'max': 10},
        {'odd_or_even': 'odd', 'max': 9}
    ]


def test_aggregate_with_simple_count_ability():
    # 想知道每一個數字有多少筆資料
    data = [1, 2, 2, 3, 3, 8, 8, 4, 3, 4, 8, 8, 10]

    aggregated = select(
        data,
        aggregate_function=len,
    )

    assert [
               {'value': 1, 'len': 1},
               {'value': 2, 'len': 2},
               {'value': 3, 'len': 3},
               {'value': 8, 'len': 4},
               {'value': 4, 'len': 2},
               {'value': 10, 'len': 1}
           ] == aggregated


@dataclass
class Developer:
    name: str
    level: str
    country: str


def test_aggregate_with_dataclass_attribute():
    # 想知道每一個職級有多少工程師
    data = [
        Developer(name="John", level="senior", country="TW"),
        Developer(name="Tom", level="senior", country="US"),
        Developer(name="Nancy", level="senior", country="TW"),
        Developer(name="Jane", level="Junior", country="US"),
        Developer(name="Jack", level="junior", country="US"),
    ]

    level_with_count = select(
        data,
        key_extraction_function=ValueAccess('level'),
        aggregate_function=len,
    )

    assert level_with_count == [
        {'len': 3, 'level': 'senior'},
        {'len': 2, 'level': 'junior'}
    ]


def test_aggregate_with_most_value():
    # 想了解，每一個職級的工程師，最多分佈那個國家
    data = [
        Developer(name="John", level="senior", country="TW"),
        Developer(name="Tom", level="senior", country="US"),
        Developer(name="Nancy", level="senior", country="TW"),
        Developer(name="Jane", level="junior", country="US"),
        Developer(name="Jack", level="junior", country="TW"),
        Developer(name="Leo", level="junior", country="US"),
    ]

    filtered = select(
        data,
        key_extraction_function=ValueAccess('level'),
        aggregate_function=MostPopularValue('country')
    )

    assert filtered == [
        {'level': 'senior', 'MostPopular:country': 'TW'},
        {'level': 'junior', 'MostPopular:country': 'US'},
    ]
