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
                 'Programming Language :: Python :: 3.7',
                 'Programming Language :: Python :: 3.8',
                 'Programming Language :: Python :: 3.9',
                 'Programming Language :: Python :: 3.10'
                ],

    keywords='pyiron',
    packages=find_packages(exclude=["*tests*", "*docs*", "*binder*", "*conda*", "*notebooks*", "*.ci_support*"]),
    install_requires=[
        'dill==0.3.4',
        'future==0.18.2',
        'gitpython==3.1.25',
        'h5io==0.1.4',
        'h5py==3.6.0',
        'numpy==1.21.5',
        'pandas==1.3.5',
        'pathlib2==2.3.6',
        'pint==0.18',
        'psutil==5.9.0',
        'pyfileindex==0.0.6',
        'pysqa==0.0.15',
        'sqlalchemy==1.4.29',
        'tables==3.6.1',
        'tqdm==4.62.3',
        'typing_extensions==4.0.1'
    ],
    cmdclass=versioneer.get_cmdclass(),

    entry_points = {
            "console_scripts": [
                'pyiron=pyiron_base.cli:main'
            ]
    }
    )
