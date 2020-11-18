from setuptools import setup

setup(
    name='event-connectors-wrapper',
    url='https://github.com/OnMyRadarInc/events-connector-wrapper',
    author='OMR',
    author_email='onmyradar.tech@gmail.com',
    packages=['event_utils', 'engines'],
    install_requires=['confluent-kafka==1.3.0'],
    version='0.4.3',
    license='MIT',
    description='A package to handle event driven engines',
    long_description=open('README.txt').read(),
)