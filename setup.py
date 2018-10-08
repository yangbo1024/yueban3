# # -*- coding:utf-8 -*-


from distutils.core import setup  


PACKAGES = ["yueban"]
NAME = "yueban"  
DESCRIPTION = "A simple distributed game server framework"
AUTHOR = "Yangbo"
AUTHOR_EMAIL = "yangbo1024@qq.com"  
URL = "https://github.com/yangbo1024/yueban_server"  
VERSION = __import__("yueban").__version__
CLASSIFIERS = [
    'License :: MIT License',
    'Development Status :: Release',
    'Programming Language :: Python',
    'Programming Language :: Python :: 3.5',
    'Programming Language :: Python :: 3.6+',
    'Operating System :: OS Independent',
    'Environment :: Web Environment',
    'Intended Audience :: Developers',
    'Topic :: Game Development',
]


def _read_lines(file_name):
    with open(file_name) as f:
        return f.readlines()


REQUIRES = _read_lines('requirements.txt')


setup(  
    name=NAME,  
    version=VERSION,  
    description=DESCRIPTION,  
    # long_description=read("README.md"),
    author=AUTHOR,  
    author_email=AUTHOR_EMAIL,  
    license="MIT",
    url=URL,  
    packages=PACKAGES,
    classifiers=CLASSIFIERS,
    install_requires=REQUIRES,
)