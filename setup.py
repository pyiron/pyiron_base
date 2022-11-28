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
                 'Programming Language :: Python :: 3.10'
                ],

    keywords='pyiron',
    packages=find_packages(exclude=["*tests*", "*docs*", "*binder*", "*conda*", "*notebooks*", "*.ci_support*"]),
    install_requires=[
        'dill==0.3.6',
        'future==0.18.2',
        'gitpython==3.1.29',
        'h5io==0.1.7',
        'h5py==3.7.0',
        'numpy==1.23.5',
        'pandas==1.5.2',
        'pathlib2==2.3.7.post1',
        'pint==0.20.1',
        'psutil==5.9.4',
        'pyfileindex==0.0.7',
        'pysqa==0.0.17',
        'sqlalchemy==1.4.44',
        'tables==3.7.0',
        'tqdm==4.64.1',
        'traitlets==5.5.0',
    ],
    cmdclass=versioneer.get_cmdclass(),

    entry_points={
            "console_scripts": [
                'pyiron=pyiron_base.cli:main'
            ]
    }
    )
