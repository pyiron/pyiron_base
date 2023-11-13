"""
Setuptools based setup module
"""
from setuptools import setup, find_packages
import versioneer

setup(
    name='pyiron_base',
    version=versioneer.get_version(),
    description='pyiron_base - an integrated development environment (IDE) for computational science.',
    long_description='http://pyiron.org',

    url='https://github.com/pyiron/pyiron_base',
    author='Max-Planck-Institut für Eisenforschung GmbH - Computational Materials Design (CM) Department',
    author_email='janssen@mpie.de',
    license='BSD',

    classifiers=['Development Status :: 5 - Production/Stable',
                 'Topic :: Scientific/Engineering :: Physics',
                 'License :: OSI Approved :: BSD License',
                 'Intended Audience :: Science/Research',
                 'Operating System :: OS Independent',
                 'Programming Language :: Python :: 3.8',
                 'Programming Language :: Python :: 3.9',
                 'Programming Language :: Python :: 3.10',
                 'Programming Language :: Python :: 3.11'
                ],

    keywords='pyiron',
    packages=find_packages(exclude=["*tests*", "*docs*", "*binder*", "*conda*", "*notebooks*", "*.ci_support*"]),
    install_requires=[
        'dill==0.3.7',
        'gitpython==3.1.40',
        'h5io==0.1.9',
        'h5py==3.10.0',
        'jinja2==3.1.2',
        'numpy==1.26.0',
        'pandas==2.1.3',
        'pint==0.22',
        'psutil==5.9.5',
        'pyfileindex==0.0.12',
        'pysqa==0.1.3',
        'sqlalchemy==2.0.23',
        'tables==3.9.1',
        'tqdm==4.66.1',
        'traitlets==5.13.0',
    ],
    cmdclass=versioneer.get_cmdclass(),

    entry_points={
            "console_scripts": [
                'pyiron=pyiron_base.cli:main'
            ]
    }
    )
