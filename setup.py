import pathlib
from setuptools import setup


# the directory containing this file
HERE = pathlib.Path(__file__).parent


# the text of the README file
README = (HERE / "README.md").read_text()

# this call to setup does all the work
setup(
    name="stringql",
    version="1.0.0",
    description="Turn string placeholders to SQL do_query parameters automatically",
    long_description=README,
    long_description_content_type="text/markdown",
    url="insert here github url",
    author="Gabriele Simeone",
    author_email="gabriele.simeone@gmail.com",
    license="MIT",
    classifiers=[
        "License :: OSI Approved :: MIT License",
        "Progamming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: PostgreSQL :: 12",
    ],
    packages=["stringql"],
    include_package_data=True,
    install_requires=["psycopg2 >= 2.8.6"],
    entry_points={},
)
