import pathlib
import setuptools

HERE = pathlib.Path(__file__).parent
README = (HERE / "README.rst").read_text()

setuptools.setup(
    name="socket_server",
    version="0.0.1",
    description="",
    long_description=README,
    long_description_content_type="text/x-rst",
    url="https://github.com/luisarboleda17/socket_server",
    author="Luis Arboleda",
    author_email="hello@larboleda.io",
    license="BSD 3-Clause",
    classifiers=[
        "Programming Language :: Python"
    ],
    packages=setuptools.find_packages(),
    python_requires=">=3"
)