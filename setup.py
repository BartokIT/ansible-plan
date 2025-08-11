from setuptools import setup, find_packages

setup(
    name="ansible-workflow",
    version="0.1.0",
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'ansible-workflow=ansible_workflow.__main__:main',
        ],
    },
)
