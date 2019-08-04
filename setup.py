from setuptools import setup

dev_requires = ["black"]
tests_require = ["pytest", "coverage", "pytest-cov"]
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
    ],
    tests_require=tests_require,
    extras_require={"dev": dev_requires, "test": tests_require},
    entry_points={"console_scripts": ["kong=kong.cli:main"]},
    packages=["kong"],
)
