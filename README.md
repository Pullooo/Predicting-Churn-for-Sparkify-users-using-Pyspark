# Predicting-Churn-for-Sparkify-users-using-Pyspark

Table of contents
* [Installation](#Installation)
* [Project background](#Project-background)
* [Blog Post](#Blog-Post)
* [Project Motivation](#Project-Motivation)
* [Files Description](#Files-Description)
* [Results](#Results)
* [Acknowledgments](#Aknowledgments)

## Installation
This program was built using a local instance of Pyspark and uses for following packages
- pyspark
- matplotlib
- seaborn
- numpy
- pandas

## Project background
Sparkify is an online streaming company that is similar to Spotify. It's millions of users stream music everyday and can be grouped up into two main categories - free-tier customers who listen to adverts between songs, and premium subscribers who listen to music advert-free but pay a premium flat rate. These users can upgrade, downgrade or cancel their service at anytime. As a result, it is important to ensure users enjoy the service.

The goal of this project is to predict customers at risk of churn. A customer churns when they put in a cancellation confirmation. Given it is far more expensie to acquire new customers than it is to retain existing ones, churn correlates with lost revenue and increased acquisition spend. By accurately predicting customers at risk of churn, the business can take proactive steps to retain the customers by incentivising them with discounts which could save Sparkify millions in revenue.

## Blog Post
Blog post associated with this notebook is found her - https://paulfru.medium.com/understanding-the-drivers-of-churn-for-a-subscription-based-businesses-6ac69037b8d1

## Project Motivation
My main motivation for choosing this project was to challenge myself to learn and practice solving a Data Science problem using Pyspark and the Machine learning best practices learned from this nanodegree.

## Files Description
- Data
    - medium-sparkify-event-data.json #input data file

- Utilities # main utility functions
- README.md

## Results
The baseline model for this project was random guess. For a dataset with 78% non churn, a random guess could get us an accuracy of 78%. 

Alghought the dataset was fairly small, the Random Forest attained both the hightest Accuracy and F1 scores - 81% and 73% respectively.

![Screenshot 2021-05-31 at 18 46 33](https://user-images.githubusercontent.com/42175485/120227137-86c9ce00-c240-11eb-9a20-e8cb456d490b.png)

Considering only a sample of the original dataset was used, scaling this ML pipeline up on the original 12G dataset should result in better model performances.

## Acknowledgements
Thanks to Udacity for making this data and project available


 

