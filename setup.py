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
                 'Programming Language :: Python :: 3.6',
                 'Programming Language :: Python :: 3.7',
                 'Programming Language :: Python :: 3.8',
                 'Programming Language :: Python :: 3.9'
                ],

    keywords='pyiron',
    packages=find_packages(exclude=["*tests*", "*docs*", "*binder*", "*conda*", "*notebooks*", "*.ci_support*"]),
    install_requires=[
        'dill==0.3.3',
        'future==0.18.2',
        'gitpython==3.1.13',
        'h5io==0.1.1',
        'h5py==3.1.0',
        'numpy==1.20.1',
        'pandas==1.2.2',
        'pathlib2==2.3.5',
        'psutil==5.8.0',
        'pyfileindex==0.0.6',
        'pysqa==0.0.15',
        'sqlalchemy==1.3.23',
        'tables==3.6.1',
        'tqdm==4.58.0'
    ],
    cmdclass=versioneer.get_cmdclass(),

    entry_points = {
            "console_scripts": [
                'pyiron=pyiron_base.cli:main'
            ]
    }
    )
