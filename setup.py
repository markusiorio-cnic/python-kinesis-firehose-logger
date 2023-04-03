from setuptools import setup

setup(name='kinesishandler',
      version='0.1',
      description='The KinesisHandler is a BufferingHandler that sends logging output to a AWS Kinesis stream',
      url='https://github.com/markusiorio-cnic/python-kinesis-logger',
      author='markusiorio',
      author_email='markus.iorio@centralnic.com',
      license='MIT',
      packages=['kinesishandler'],
      zip_safe=False)
