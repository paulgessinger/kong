import typing

class And:
    def __init__(self, *args: typing.Any, **kwargs: typing.Any): ...

class Optional:
    def __init__(self, *args: typing.Any, **kwargs: typing.Any): ...

class Schema:
    def __init__(self, *args: typing.Any, **kwargs: typing.Any):
        ...

    def validate(self, arg: typing.Any) -> typing.Any:
        ...

    def is_valid(self, arg: typing.Any) -> bool:
        ...

