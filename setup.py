from setuptools import setup
from catkin_pkg.python_setup import generate_distutils_setup

d = generate_distutils_setup(
    packages=['rosbag', 'ROSfs'],
    package_dir={'': 'src'},
    scripts=['scripts/rosbag', 'scripts/bag2rosfs', 'scripts/rosfs'],
    requires=['genmsg', 'genpy', 'roslib', 'rospkg', 'pyzmq']
)

setup(**d)