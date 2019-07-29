from setuptools import setup, find_packages

with open('./requirements.txt') as reqs:
    requirements = [line.rstrip() for line in reqs]

setup(name="batch-cog",
      version='1.0',
      author='Jeff Albrecht (Flyalchemy)',
      author_email='geospatialjeff@gmail.com',
      packages=find_packages(exclude=['docs']),
      install_requires = requirements,
      entry_points= {
          "console_scripts": [
              "batch-cog=batch_cog.cli:batch_cog"
          ]},
      include_package_data=True
      )