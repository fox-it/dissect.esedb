from setuptools import find_packages, setup

setup(
    name="dissect.esedb",
    packages=list(map(lambda v: "dissect." + v, find_packages("dissect"))),
    install_requires=[
        "dissect.cstruct>=3.4.dev,<4.0.dev",
        "dissect.util>=3.5.dev,<4.0.dev",
    ],
)
