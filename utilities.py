from pyspark.sql.functions import lit, col, max, min, avg, stddev,count
import matplotlib.pyplot as plt

def get_counts_pddf(column, inputDf, idField):
    """gets value counts and converts dataframe to pandas dataframe
    
    Input
    column: categorical column for which to obtain counts
    inputDf: input dataframe
    idField: field on which the count is done
    
    Output
    Returns a pandas dataframe indexed on the categorical column"""
    pd_summary_df = inputDf.groupBy(column).agg(count(idField).alias('count')).sort(col('count').desc()).toPandas()
    pd_summary_df.set_index(column, inplace = True)
    
    return pd_summary_df
    
def bar_plot(inputDf, title):
    """generates annotated bar plot from pandas dataframe with proportions as annotations
    
    input
    inputDf: input dataframe containing categorical field as index and counts as values
    """
    numCategories = len(inputDf)
    
    if numCategories<=7:
        ax = inputDf.plot(kind = 'bar')
    elif numCategories >7 and numCategories <=25:
        ax = inputDf.plot(kind = 'bar', figsize=(14,6))
    else:
        print('There are too many categories: {} in all'.format(numCategories))
        print(inputDf)
    plt.title(title)

    totals = []

    for i in ax.patches:
        totals.append(i.get_height())

    tot = sum(totals)
    for i in ax.patches:
        ax.text(x=i.get_x() + 0.17, y=i.get_height(), s='{}%'.format(round(i.get_height()/tot * 100,0)))
        
        
#propensity to churn
def churn_per_10k(inputDf, catCol, churnCol):
    """For each value in col, this function derives how many users churn out of every 10000 users
    
    input:
    inputDf: dataframe containing churn marker
    catCol: (str) categorical field
    churnCol: (str) column containing churn marker
    
    output:
    Data frame containing number of customer who churn in every 10000 users"""
    pd_agg = inputDf.groupBy(catCol,churnCol).agg(count('userId').alias('count')).dropna().toPandas()
    pd_agg.set_index([catCol,churnCol], inplace=True)
    temp = pd_agg.unstack()
    temp.columns = ['noChurn','churn']
    temp['churnPer10k'] = round(temp.churn/temp.sum(axis=1) * 10000,2)
    return temp

def plot_churnper10k(inputDf, churnPer10k):
    """plot and annotates bar plots showing number of churn per 10k users by values
    input:
    inputDf: datafrmae containing churnPer10k and indexed on the the variable
    churnPer10k: (str) field holding churnPer10k"""
    numCategories = len(inputDf)
    
    if numCategories<=7:
        ax = inputDf[churnPer10k].plot(kind = 'bar')
    elif numCategories >7 and numCategories <=25:
        ax = inputDf[churnPer10k].plot(kind = 'bar', figsize=(14,6))

    plt.title('Out of every 10,000 interactions, how many churn?')

    for i in ax.patches:
        ax.text(x=i.get_x() + 0.17, y=i.get_height(), s=i.get_height())
        

def bar(inputDf, title):
    """
    Generates a bar plot with annotations
    
    input
    inputDf: Dataframe of churn by an aggregated field, with dataframe indexed on churn
    """
    ax = inputDf.plot(kind='bar')
    ax.set_title(title)
    ax.get_legend().remove()

    for i in ax.patches:
        ax.text(x=i.get_x()+ 0.17, y=i.get_height(), s=i.get_height())
        
def agg_by_churn(inputDf, churnCol, numericCol):
    """Finds the mean values of a numeric field by churn and outputs a pandas dataframe indexed on churn
    
    input
    inputDf: input dataframe. Must contain churn and field we want to summarise
    churnCol: (str) Churn column
    numericCol: (str) numeric field to summarise
    
    output: pandas dataframe with churn as index
    """
    
    pd_df = inputDf.groupby(churnCol).agg({numericCol:'mean'}).toPandas()
    pd_df.set_index('churn', inplace=True)
    pd_df.columns = ['avg {}'.format(numericCol)]
    pd_df = round(pd_df,2)
    return pd_df