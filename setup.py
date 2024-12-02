from setuptools import setup

setup(
    name = 'spacescans',
    version = '0.1.0',   
    description = 'A Python package to conduct individual exposome linkage',
    url = 'https://github.com/uf-hobi-informatics-lab/SPACESCANS.git',
    author = [
        { name = "Jiang Bian", email = "bianjiang@ufl.edu" },
    ],
    maintainers = [
        { name = "Jason Glover", email = "jasonglover@ufl.edu" },
    ],
    license = {file = "LICENSE.txt"}, 
    readme = "README.md",
    packages = ['spacescans'],
    requires-python = ">= 3.8",
    install_requires = ['numpy',
                      'pandas',
                      'sqlalchemy',
                      'os',
                      'datetime',
                       ],

    classifiers = [
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',        
        'Programming Language :: Python :: 3',
    ],
)