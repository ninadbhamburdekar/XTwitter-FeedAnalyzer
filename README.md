# XTwitter-FeedAnalyzer

Python scripts for collection of activity on X based on tweet content, and user connections.

The scripts need an elasticsearch backend for data storage in the following indexes:

USERINDEX - Stores User information related to tweets

DATAINDEX - Stores Tweet Content

OPSINDEX - Stores operational data for maintaining state etc.

Data is stored in DATAINDEX on elasticsearch in the following form:

![image](https://whiteclouddrive.com/terms/store/res/wy0lhz7pc5vcuh09in3vmp7u2/img.jpg)

