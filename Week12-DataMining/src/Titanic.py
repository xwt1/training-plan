import numpy as np # linear algebra
import pandas as pd # data processing, CSV file I/O (e.g. pd.read_csv)

import matplotlib
import matplotlib.pyplot as plt
import seaborn as sns
sns.set(color_codes = True)
%matplotlib inline

##### Scikit Learn modules needed for Logistic Regression
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import confusion_matrix
from sklearn.preprocessing import LabelEncoder,MinMaxScaler , StandardScaler

import os
for dirname, _, filenames in os.walk('/kaggle/input'):
    for filename in filenames:
        print(os.path.join(dirname, filename))

# 1.读训练集和测试集合
train_df =pd.read_csv("../input/titanic/train.csv")
test_df = pd.read_csv("../input/titanic/test.csv")

# full_df = train_df.append(test_df, ignore_index=True)   #将训练集和测试集放在一起,之后再分开
full_df = pd.concat([train_df, test_df], ignore_index=True)
print(f'There are {full_df.shape[0]} rows and {full_df.shape[1]} columns in the full dataframe.')

full_df.head() #展示一下要用于训练的数据

full_df.info() #展示一下每一个特征的类型

full_df.describe() #展示对于每一个特征的平均值,最小值等数值特征,并对其进行分析

# 2.从这里开始做预处理,处理数据
def gender_process(x):
    if x == 'female':
        return 1
    else:
        return 0

df_clean = full_df
df_clean['Sex'] = df_clean['Sex'].apply(gender_process) #将性别那一行转为0与1

# 3 相关性数据可视化(数据分析)
# correlation = df_clean.corr() #相关性矩阵
numeric_columns = df_clean.select_dtypes(include='number')
correlation = numeric_columns.corr()

plt.figure(figsize=(15,10) )
sns.heatmap(correlation,annot = True,cmap = 'Blues')

# 4 主观降维，将看起来与survive相关性不大的维度全部扔掉(特征工程)：
df_clean.drop(['PassengerId','Name','SibSp','Parch','Ticket','Cabin','Embarked'],axis='columns',inplace=True)
df_clean.head() #看一眼数据清洗后一部分数据的样子

# 2.3 清洗掉数据中为空的值(也就是某名乘客没有相关特征)(数据清洗)
df_clean.isnull().sum()
df_clean1 = df_clean.dropna()
df_clean1.isnull().sum()

# 2.4 设置输入集合,以及输出集合(inputs和target都是训练集合的一部分)
inputs = df_clean1.drop('Survived',axis='columns')
target = df_clean1.Survived

# 3。 设置训练集,将数据集分为训练和测试集合两个部分(区分训练集和测试集)
from sklearn.model_selection import train_test_split
X_train, X_test, y_train, y_test = train_test_split(inputs,target,test_size=0.35,random_state= 20)

# 4. 构建逻辑斯蒂回归模型(模型构建与模型评估)
logistic = LogisticRegression()
logistic.fit(X_train, y_train)
y_predicted = logistic.predict(X_test)
score = logistic.score(X_test, y_test)
print("逻辑斯蒂回归准确率: "+str(score))

# 4. 构建朴素贝叶斯模型(模型构建与模型评估)
from sklearn.naive_bayes import GaussianNB
Bayes = GaussianNB()
Bayes.fit(X_train, y_train)
y_predicted = Bayes.predict(X_test)
score = Bayes.score(X_test, y_test)
print("贝叶斯准确率: "+str(score))

# 4.构建随机森林模型(模型构建与模型评估)
from sklearn.ensemble import RandomForestClassifier
randomforest = RandomForestClassifier()
randomforest.fit(X_train, y_train)
y_predicted = randomforest.predict(X_test)
score = randomforest.score(X_test, y_test)
print("随机森林准确率: "+str(score))

# 4.构建KNN(最近K邻居)(模型构建与模型评估)
from sklearn.neighbors import KNeighborsClassifier

knn = KNeighborsClassifier()
knn.fit(X_train,y_train)
y_predicted=knn.predict(X_test)
score= knn.score(X_test, y_test)
print("knn准确率: "+str(score))

# 4.构建决策树(模型构建与评估)
from sklearn.tree import DecisionTreeClassifier

dtc = DecisionTreeClassifier()
dtc.fit(X_train,y_train)
y_predicted=dtc.predict(X_test)
score= dtc.score(X_test, y_test)
print("决策树准确率: "+str(score))

# 4.构建支持向量机
from sklearn.svm import SVC

svc = SVC()
svc.fit(X_train,y_train)
y_predicted=svc.predict(X_test)
score= svc.score(X_test, y_test)
print("svc准确率: "+str(score))

# 4.构建袋装模型
from sklearn.ensemble import BaggingClassifier
bagging_classifier = BaggingClassifier()
bagging_classifier.fit(X_train,y_train)
y_predicted=bagging_classifier.predict(X_test)
score= bagging_classifier.score(X_test, y_test)
print("bagging准确率: "+str(score))