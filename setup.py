from setuptools import setup

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
        "attrdict",
    ],
    extras_require={"dev": ["black"]},
    entry_points={"console_scripts": ["kong=kong.cli:main"]},
    packages=["kong"],
)
