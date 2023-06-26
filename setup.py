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
        'dill==0.3.6',
        'gitpython==3.1.31',
        'h5io==0.1.7',
        'h5py==3.9.0',
        'numpy==1.24.3',
        'pandas==2.0.2',
        'pathlib2==2.3.7.post1',
        'pint==0.22',
        'psutil==5.9.5',
        'pyfileindex==0.0.11',
        'pysqa==0.0.25',
        'sqlalchemy==2.0.16',
        'tables==3.8.0',
        'tqdm==4.65.0',
        'traitlets==5.9.0',
    ],
    cmdclass=versioneer.get_cmdclass(),

    entry_points={
            "console_scripts": [
                'pyiron=pyiron_base.cli:main'
            ]
    }
    )
