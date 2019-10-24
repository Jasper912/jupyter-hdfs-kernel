from setuptools import setup, find_packages
from os import path


here = path.abspath(path.dirname(__file__))


with open(path.join(here, 'requirements.txt')) as f:
    requirements = f.read().splitlines()

with open(path.join(here, 'README.md')) as f:
    long_description = f.read()


from hdfs_kernel.constants import __version__, DISPLAY_NAME

setup(
    name="HdfsKernel",
    version=__version__,
    description=DISPLAY_NAME + ' Kernel',
    long_description=long_description,
    url='https://github.com/Jasper912/jupyter_hdfs_kernel.git',
    author='huangnj',
    author_email = "jasper0912@icloud.com",
    license='MIT License',
    keywords='Hdfs Pyhdfs Kernel Ipykernel',
    packages=find_packages(),
    install_requires=requirements,
    include_package_data=True,
    package_data={
        'hdfs_kernel': [
            "kernels/hdfs/kernel.js",
            "kernels/hdfs/kernel.json",
        ]
    },
)
