from setuptools import setup, find_packages  # type: ignore

dev_requires = ["black"]
tests_require = ["pytest", "coverage", "pytest-cov", "mypy", "flake8", "tox"]
setup(
    name="kong-batch",
    use_scm_version=True,
    setup_requires=["setuptools_scm"],
    description="",
    url="http://github.com/paulgessinger/kong",
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
        "notifiers",
        "jinja2",
    ],
    tests_require=tests_require,
    extras_require={"dev": dev_requires, "test": tests_require, "ipython": ["ipython"]},
    entry_points={"console_scripts": ["kong=kong.cli:main"]},
    packages=find_packages("src"),
    package_dir={"": "src"},
)
