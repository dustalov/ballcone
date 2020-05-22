import os
import re
from typing import List

from setuptools import setup


def get_package_variable(name: str, rel_path='ballcone/__init__.py') -> str:
    path = os.path.join(os.path.abspath(os.path.dirname(__file__)), rel_path)

    pattern = re.compile(r'^{}.*?([\'"])(?P<value>.+)\1.*$'.format(re.escape(name)))

    with open(path, 'r', encoding='UTF-8') as f:
        for line in f:
            match = pattern.match(line)

            if match:
                return match.group('value')
        else:
            raise RuntimeError('Unable to find variable: ' + name)


__version__ = get_package_variable('__version__')
__license__ = get_package_variable('__license__')

with open('README.md', 'r', encoding='UTF-8') as f:
    long_description = f.read()

with open('requirements.txt', 'r', encoding='UTF-8') as f:
    install_requires = f.read()


def package_files(directory) -> List[str]:
    paths = []

    for (path, _, filenames) in os.walk(directory):
        for filename in filenames:
            paths.append(os.path.join('..', path, filename))

    return paths


package_data = {'': package_files('ballcone/templates')}

setup(name='ballcone',
      version=__version__,
      description='Ballcone is a fast and lightweight server-side Web analytics solution.',
      long_description=long_description,
      long_description_content_type='text/markdown',
      url='https://github.com/dustalov/ballcone',
      author='Dmitry Ustalov',
      license=__license__,
      packages=['ballcone'],
      package_data=package_data,
      entry_points={'console_scripts': ['ballcone = ballcone.__main__:main']},
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Intended Audience :: Developers',
          'Intended Audience :: Information Technology',
          'License :: OSI Approved :: MIT License',
          'Operating System :: OS Independent',
          'Programming Language :: Python :: 3',
          'Topic :: Database',
          'Topic :: Internet :: Log Analysis',
          'Topic :: Internet :: WWW/HTTP',
          'Typing :: Typed'
      ],
      keywords=['Web analytics', 'log analysis', 'columnar storage', 'syslog', 'nginx'],
      install_requires=install_requires,
      zip_safe=True)
