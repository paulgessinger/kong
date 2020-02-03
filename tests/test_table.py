import click
import pytest
from click import unstyle

from kong.table import format_table, _do_align


def test_no_stretch():
    headers = ["alpha", "beta", "gamma"]
    rows = [
        ["delta", "omega", "psi"],
        ["echo", "charlie", "bingo"],
        ["nebukadnezar", "otto", "kilo"],
    ]
    align = ["r", "c", "l"]

    s = format_table(headers, rows, align, width=None)
    print(s)

    assert (
        s
        == """
       alpha  beta   gamma
------------ ------- -----
       delta  omega  psi  
        echo charlie bingo
nebukadnezar  otto   kilo 
"""[
            1:-1
        ]
    )

    s = format_table(headers, rows, align, width=80)
    print(s)

    assert (
        s
        == """
       alpha  beta   gamma
------------ ------- -----
       delta  omega  psi  
        echo charlie bingo
nebukadnezar  otto   kilo 
"""[
            1:-1
        ]
    )


def test_stretch():
    headers = ["alpha", "beta", "gamma"]
    rows = [
        ["delta", "omega", "psi"],
        ["echo", "charlie", "bingo"],
        ["nebukadnezar", "otto", "kilo"],
    ]
    align = ["r+", "c", "l"]
    s = format_table(headers, rows, align, width=80)
    assert (
        s
        == """
                                                             alpha  beta   gamma
------------------------------------------------------------------ ------- -----
                                                             delta  omega  psi  
                                                              echo charlie bingo
                                                      nebukadnezar  otto   kilo 
"""[
            1:-1
        ]
    )

    for line in s.split("\n"):
        assert len(line) == 80

    align = ["l", "r+", "l"]
    s = format_table(headers, rows, align, width=100)
    assert (
        s
        == """
alpha                                                                                     beta gamma
------------ --------------------------------------------------------------------------------- -----
delta                                                                                    omega psi  
echo                                                                                   charlie bingo
nebukadnezar                                                                              otto kilo 
"""[
            1:-1
        ]
    )

    for line in s.split("\n"):
        assert len(line) == 100

    align = ["r+", "r", "l"]
    s = format_table(headers, rows, align, width=100)
    print()
    print(s)
    assert (
        s
        == """
                                                                                 alpha    beta gamma
-------------------------------------------------------------------------------------- ------- -----
                                                                                 delta   omega psi  
                                                                                  echo charlie bingo
                                                                          nebukadnezar    otto kilo 
"""[
            1:-1
        ]
    )

    for line in s.split("\n"):
        assert len(line) == 100

    rows = [
        ["delta", "omega", "psi"],
        ["echo", "charlie tango tina purple rain", "bingo"],
        ["nebukadnezar", "otto", "kilo"],
    ]
    align = ["l", "c+", "l"]
    s = format_table(headers, rows, align, width=40)
    print(s)

    assert (
        s
        == """
alpha                beta          gamma
------------ --------------------- -----
delta                omega         psi  
echo         charlie t...rple rain bingo
nebukadnezar         otto          kilo 
"""[
            1:-1
        ]
    )

    for line in s.split("\n"):
        assert len(line) == 40


def test_colors():
    headers = ["alpha", "beta", click.style("gamma", bg="green")]
    rows = [
        ["delta", "omega", "psi"],
        ["echo", click.style("charlie", fg="red"), "bingo"],
        [click.style("nebukadnezar", bold=True), "otto", "kilo"],
    ]
    align = ["r", "c", "l"]

    s = format_table(headers, rows, align, width=None)
    print()
    print(s)

    assert (
        unstyle(s)
        == """
       alpha  beta   gamma
------------ ------- -----
       delta  omega  psi  
        echo charlie bingo
nebukadnezar  otto   kilo 
"""[
            1:-1
        ]
    )

    align = ["r+", "c", "l"]

    s = format_table(headers, rows, align, width=100)
    print()
    print(s)

    assert (
        unstyle(s)
        == """
                                                                                 alpha  beta   gamma
-------------------------------------------------------------------------------------- ------- -----
                                                                                 delta  omega  psi  
                                                                                  echo charlie bingo
                                                                          nebukadnezar  otto   kilo 
"""[
            1:-1
        ]
    )

def test_stretch_shorten():
    headers = ["alpha", "beta", click.style("gamma", bg="green")]
    rows = [
        ["delta", "omega", "psi"],
        ["echo", click.style("charlie", fg="red"), "bingo"],
        [click.style("nebukadnezar prometheus apollo jupiter", bold=True), "otto", "kilo"],
    ]
    align = ["r+", "c", "l"]
    s = format_table(headers, rows, align, width=50)
    print()
    print(s)

    assert (
        unstyle(s)
        == """
                               alpha  beta   gamma
------------------------------------ ------- -----
                               delta  omega  psi  
                                echo charlie bingo
nebukadnezar pro...us apollo jupiter  otto   kilo 
"""[
           1:-1
           ]
    )


def test_do_align():
    s = "abcabc"
    assert "abcabc    " == _do_align(s, "l", 10, " ")
    assert "    abcabc" == _do_align(s, "r", 10, " ")
    assert "  abcabc  " == _do_align(s, "c", 10, " ")
    assert " abcxabc  " == _do_align("abcxabc", "c", 10, " ")
    with pytest.raises(ValueError):
        _do_align("abcabc", "k", 10, " ")

    s = "long long long long"
    assert 10 == len(_do_align(s, "r", 10, " "))
    s = click.style("nebukadnezar prometheus apollo jupiter", bold=True)
    out = _do_align(s, "r", 36, " ")
    assert len(click.unstyle(out)) == 36
    assert click.unstyle(out) == "nebukadnezar pro...us apollo jupiter"
    assert click.unstyle(out) != out

    s = click.style("abcabc", fg="red")
    assert _do_align(s, "l", 10, " ") == f"{s}    "
    assert _do_align(s, "r", 10, " ") == f"    {s}"
    assert _do_align(s, "c", 10, " ") == f"  {s}  "
    s = click.style("abcxabc", fg="red")
    assert _do_align(s, "c", 10, " ") == f" {s}  "

