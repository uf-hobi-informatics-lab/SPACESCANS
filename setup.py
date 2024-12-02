from setuptools import setup

setup(
    name = 'spacescans',
    version = '0.1.0',   
    description = 'A Python package to conduct individual exposome linkage',
    url = 'https://github.com/uf-hobi-informatics-lab/SPACESCANS.git',
    author='Jiang Bian',
    author_email='bianjiang@ufl.edu',
    maintainer='Jason Glover',
    maintainer_email='jasonglover@ufl.edu',
    license = 'MIT',
    long_description=open('README.md').read(),  
    long_description_content_type='text/markdown',  
    python_requires='>=3.8',
    readme = "README.md",
    packages = ['spacescans'],
    install_requires = [
        'numpy',
        'pandas',
        'sqlalchemy',
    ],

    classifiers = [
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',        
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
)