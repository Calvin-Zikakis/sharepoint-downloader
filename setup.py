from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="sharepoint-backup-tool",
    version="1.0.0",
    author="Calvin Zikakis",
    author_email="zikakis.calvin@gmail.com",
    description="A robust tool for backing up SharePoint Online sites",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Calvin-Zikakis/sharepoint-downloader",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: System Administrators",
        "Topic :: System :: Archiving :: Backup",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.7",
    install_requires=[
        "O365>=2.0.0",
    ],
    entry_points={
        "console_scripts": [
            "sharepoint-backup=sharepoint_backup:main",
            "sharepoint-monitor=monitor:main",
        ],
    },
    include_package_data=True,
    package_data={
        "": ["*.sample", "*.template"],
    },
)