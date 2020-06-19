import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="dataforest", # Replace with your own username
    version="0.0.1",
    author="Austin McKay",
    author_email="austinmckay303@gmail.com",
    description="An interactive data science workflow manager",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/TheAustinator/dataforest",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)