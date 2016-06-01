from setuptools import setup, find_packages

setup(
    name='barin',
#    version='0.0.5',
    version_format='{tag}.dev{commitcount}',
    setup_requires=['setuptools-git-version'],
    description='Barin, yet another MongoDB schema validation layer',
    long_description='Some restructured text maybe',
    classifiers=[
        "Programming Language :: Python",
    ],
    author='Rick Copeland',
    author_email='rick@arborian.com',
    url='',
    keywords='',
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[],
    tests_require=[],
    entry_points="""
    [console_scripts]
    """)
