from setuptools import find_packages, setup

from pathlib import Path

with open("README.md", "r") as fh:
    long_description = fh.read()


def parse_requirements(requirements_path):
    with open(Path(__file__).parent / requirements_path) as f:
        return f.read().splitlines()


requirements = parse_requirements("requirements.txt")
test_requirements = parse_requirements("requirements-test.txt")
dev_requirements = parse_requirements("requirements-dev.txt")

setup(
    name="dataforest",  # Replace with your own username
    version="0.0.1",
    author="Austin McKay",
    author_email="austinmckay303@gmail.com",
    description="An interactive data science workflow manager",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/TheAustinator/dataforest",
    packages=find_packages(),
    # TODO: this works in "all", but not in `install_reqs` -- fix
    install_requires=requirements,
    extras_require={"all": requirements, "test": test_requirements, "dev": dev_requirements,},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: AGPL 3.0 License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.6",
)
