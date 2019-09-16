from setuptools import setup  # type: ignore

dev_requires = ["black"]
tests_require = ["pytest", "coverage", "pytest-cov", "mypy", "flake8"]
setup(
    name="kong",
    version="0.1.0",
    description="",
    url="http://github.com/paulgessinger/futile",
    author="Paul Gessinger",
    author_email="hello@paulgessinger.com",
    license="MIT",
    install_requires=[
        "click",
        "pyyaml",
        "halo",
        "sh",
        "python-dateutil",
        "peewee",
        "coloredlogs",
        "humanfriendly",
        "psutil",
        "schema",
        "tqdm",
    ],
    tests_require=tests_require,
    extras_require={"dev": dev_requires, "test": tests_require, "ipython": ["ipython"]},
    entry_points={"console_scripts": ["kong=kong.cli:main"]},
    packages=["kong"],
)
