from enum import Enum

class Period(Enum):
    ONE_DAY = 1
    TWO_DAYS = 2
    THREE_DAYS = 3
    FOUR_DAYS = 4
    FIVE_DAYS = 5
    SIX_DAYS = 6
    SEVEN_DAYS = 7
    EIGHT_DAYS = 8
    NINE_DAYS = 9
    TEN_DAYS = 10
    ONE_WEEK = 7
    TWO_WEEKS = 14
    THREE_WEEKS = 21
    FOUR_WEEKS = 28
    FIVE_WEEKS = 35
    SIX_WEEKS = 42
    SEVEN_WEEKS = 49

class CallPut(Enum):
    CALL = ("c", 1)
    PUT = ("p", -1)

class DayOfWeek(Enum):
    MONDAY = 0
    TUESDAY = 1
    WEDNESDAY = 2
    THURSDAY = 3
    FRIDAY = 4
    SATURDAY = 5
    SUNDAY = 6
