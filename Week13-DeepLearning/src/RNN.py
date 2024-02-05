# import numpy as np
# # from activator.softmax import SoftMax
# # from activator.tanh import Tanh
#
# from scipy.special import softmax
#
# '''
#     @author: liuhaibing
# '''
#
#
# # 手写循环神经网络
# # 全连接层只用三层input-hidden-output
#
# class RNN():
#
#     # def __init__(self, inputSize, stateSize, outputSize, times=1, maxLen=32,
#     #              activator=SoftMax, learningRate=0.01):
#     def __init__(self, inputSize, stateSize, outputSize, times=1, maxLen=32,
#                   learningRate=0.01):
#         '''
#         定义网络结构
#         :param inputSize: 输入x向量的长度
#         :param stateSize: 其实就是s的大小，s其实就是之前dense的hidden层神经元个数
#         :param times: 记录时间状态，记录多长时间的状态
#         :param maxLen: 最大记忆长度 解决记忆的太长导致梯度消失
#         '''
#         self.inputSize = inputSize
#         self.stateSize = stateSize
#         self.outputSize = outputSize
#         # self.activator = activator
#
#         self.Ux = np.random.uniform(-0.5, 0.5, (stateSize, inputSize))
#         self.Ws = np.random.uniform(-0.5, 0.5, (stateSize, stateSize))
#         self.Vy = np.random.uniform(-0.5, 0.5, (outputSize, stateSize))
#
#         self.Wb = np.zeros(stateSize)
#         self.Vb = np.zeros(outputSize)
#         # 记录状态信息
#         self.stateList = []  # 有少条样本就有产生多少条状态
#         self.times = times  # times是记录的计算次数，一个轮次内，最大是系列总长度
#         self.maxLen = maxLen  # 暂时不用吧
#         self.outputList = []
#         pass
#
#     # 正向计算求结果值
#     def forward(self, inputX, times, maxLen):
#         '''
#         :param inputX: 输入的系列，是向量
#         :param times:  是时间，初始值为1，说明是第一个样本的计算
#         :return:
#         '''
#         if times - 1 == 0:  # t0时刻对stateList，outputList重置，说明一轮训练结束
#             self.stateList = []
#             self.outputList = []
#             self.stateList.append(np.zeros(self.stateSize))
#             self.outputList.append(np.zeros(self.outputSize))
#             pass
#
#         # 此处要用tanh激活
#         state = np.dot(self.Ux, inputX) + np.dot(self.Ws, self.stateList[times - 1]) + self.Wb
#         # tanh = Tanh()
#         # state = tanh.forward(state)
#         state = np.tanh(state)
#         self.stateList.append(state)  # 记录当前时刻的状态
#         # 此处要用softmax激活
#         output = np.dot(self.Vy, state) + self.Vb  # 记录t时刻的输出
#
#         # softmax = SoftMax()
#
#         output = softmax(output)
#
#         self.outputList.append(output)  # 记录t时刻的输出
#         self.times = times  # 下一个时刻
#         return output
#         pass
#
#     # 反向传播求梯度
#     def backward(self, X, inputY, T):
#         # 分别求Vy，Ws、Ux以及Wb和Vb的梯度
#         # 其中Vy，Ux和Vb的梯度可以直接算出来，唯一就是Ws计算比较复杂
#         # loss函数使用 E(y) = -np.sum(y * log(y) + (1 - y)*log)
#         # 计算各个时刻的delta，最后T时刻的delta = V(Y(T) - y(T))
#         deltaT = self.Vy.dot(self.outputList[T] - inputY[T - 1])
#         deltaList = [0 for i in range(0, T + 1)]
#         deltaList[T] = deltaT
#         # 计算1到T-1时刻的delta：
#         for t in range(T - 1, 0, -1):
#             deltat = self.Vy.dot(self.outputList[t] - inputY[t - 1]) \
#                      + (self.Ws.dot(np.diag(1 - self.stateList[t + 1] ** 2))).dot(deltaList[t + 1])
#             deltaList[t] = deltat
#             pass
#
#         # 计算loss/Vb loss/Yy的梯度
#         self.vbGrad = np.zeros(shape=self.Vb.shape)  # 按行求和，返回的是向量
#         self.vyGrad = np.zeros(shape=self.Vy.shape)  # 按行求和，返回的是向量
#
#         # 计算loss/Ws loss/Wb loss/Ux
#         self.wsGrad = np.zeros(shape=self.Ws.shape)
#         self.wbGrad = np.zeros(shape=self.Wb.shape)
#         self.uxGrad = np.zeros(shape=self.Ux.shape)
#         for t in range(1, T + 1):
#             self.vbGrad += self.outputList[t] - inputY[t - 1]
#             self.vyGrad += np.dot((self.outputList[t] - inputY[t - 1])[:, np.newaxis], self.stateList[t][np.newaxis, :])
#
#             self.wsGrad += np.dot(np.dot(np.diag(1 - self.stateList[t] ** 2), deltaList[t])[:, np.newaxis],
#                                   self.stateList[t - 1][np.newaxis, :])
#             self.wbGrad += np.dot(np.diag(1 - self.stateList[t] ** 2), deltaList[t])
#             self.uxGrad += np.dot(np.dot(np.diag(1 - self.stateList[t] ** 2), deltaList[t])[:, np.newaxis],
#                                   X[t - 1][np.newaxis, :])
#             pass
#         pass
#
#     def update(self, learningRate):
#         '''
#         根据学习速率和已经获得的梯度，更新权重系数和截距项的梯度
#         :param learningRate:
#         :return:
#         '''
#         print(self.Ws)
#         self.Ux -= self.uxGrad * learningRate
#         self.Ws -= self.wsGrad * learningRate
#         self.Vy -= self.vyGrad * learningRate
#
#         self.Wb -= self.wbGrad * learningRate
#         self.Vb -= self.vbGrad * learningRate
#
#         pass
#
#     def predict(self, input):
#         '''
#         正向计算，预测结果
#         :param input: 是一个向量
#         :return:
#         '''
#         output = self.forward(input, 1, 12)
#
#         return output
#         pass
#
#     def fit(self, X, Y, loss=None, epochs=1000, learningRate=0.1):
#         '''
#         训练模型
#         :param X:
#         :param y:
#         :param loss: 成本函数
#         :return:
#         '''
#         T = len(X)
#         for i in range(epochs):
#             # epochs
#             print("epochs:", i)
#             times = 1
#             # 正向计算所有的state和output
#             for x, y in zip(X, Y):
#                 self.forward(x, times, T)  # T是系列的总长度，暂时不考虑截取
#                 times += 1
#                 pass
#             # 反向传播求梯度
#             self.backward(X, Y, T)
#             # 跟新梯度
#             self.update(learningRate)
#             pass
#         pass
#
#     pass
#
#
# rnn = RNN(4, 4, 4)
#
# # 构造训练样本
# #  我 是 中国 人
# # 我->   [1,0,0,0]
# # 是->   [0,1,0,0]
# # 中国-> [0,0,1,0]
# # 人->   [0,0,0,1]
# X = np.array([
#     [1, 0, 0, 0],
#     [0, 1, 0, 0],
#     [0, 0, 1, 0],
#     [0, 0, 0, 1],
#     [1, 0, 0, 0],
#     [0, 1, 0, 0],
#     [0, 0, 1, 0],
#     [0, 0, 0, 1],
#     [1, 0, 0, 0],
#     [0, 1, 0, 0],
#     [0, 0, 1, 0],
#     [0, 0, 0, 1]
# ])
# y = np.array([
#     [0, 1, 0, 0],
#     [0, 0, 1, 0],
#     [0, 0, 0, 1],
#     [0, 0, 0, 0],
#     [0, 1, 0, 0],
#     [0, 0, 1, 0],
#     [0, 0, 0, 1],
#     [0, 0, 0, 0],
#     [0, 1, 0, 0],
#     [0, 0, 1, 0],
#     [0, 0, 0, 1],
#     [0, 0, 0, 0],
# ])
# rnn.fit(X, y)
# print(rnn.predict([1, 0, 0, 0]), 1, 12)


import numpy as np
import pandas as pd
import math




def sigmoids(self, x):
    return 1 / (1 + math.exp(x))


def softmax(x):
    exp_x = np.exp(x - np.max(x))  # 减去最大值，防止指数爆炸
    sum_exp_x = np.sum(exp_x, axis=-1, keepdims=True)
    return exp_x / sum_exp_x


class RNN:
    """
        分类问题公式:
        z_t = W * h_{t-1} + V * x_t + b_w
        h_t = tanh(z_t)
        o_t = V * h_t + b_v
        y^hat_{t} =softmax(o_t)
        loss_t = - (y_t)^T * log(y^hat_{t})
        其中loss_t是一个标量,其余小写字母代表列向量,大写字母代表矩阵


        利用最简单的公式来书写:h_t = tanh(W_{ih}x_t + W_hhh_{t-1}+b_{hh})
        每一层的实际输出再加一层全连接层后再包裹一层softmax层,即y_t = softmax(U*h_t + b_{vv})
        直接使用多元交叉熵作为损失函数,即Loss = -Sum_i y_i log(y_hat_i) ,其中y是标签值,y^hat预测值
    """
    """
        分类问题初始化
        input_size: 公式中x_t(一般为词向量,也就是词嵌入矩阵的一行)的维度为1*input_size
        hidden_size: 公式中h_t为隐藏层
        输出层h_i的维度为 1*output_size (output_size为分类的大小)
        time_stamps为时间戳的数量,即RNN单元的数量
    """

    def __init__(self, input_size,hidden_size, output_size, time_stamps, lr=0.01):
        # 设置随机种子,保证随机化
        np.random.seed(1)
        # 存下输入输出的维度以及隐藏层的维度
        self.input_size = input_size
        self.hidden_size = hidden_size
        self.output_size = output_size
        self.time_stamps = time_stamps
        # 随机初始化权重和偏置
        w_shape = (hidden_size, hidden_size)
        self.W = np.random.laplace(0, 1, w_shape)
        u_shape = (hidden_size, input_size)
        self.U = np.random.laplace(0, 1, u_shape)
        v_shape = (output_size, hidden_size)
        self.V = np.random.laplace(0, 1, v_shape)
        bw_shape = (hidden_size, 1)
        self.bw = np.random.laplace(0, 1, bw_shape)
        bv_shape = (output_size, 1)
        self.bv = np.random.laplace(0, 1, bv_shape)
        # 创建一个字典用于装入反向传播时需要的一些量值
        self.cache = {}
        self.cache["y_hat_list"] = []
        self.cache["h_t_list"] = []
        self.cache["z_t_list"] = [] 
        self.cache["x_t_list"] = [] 

    # 前向传播部分
    """
        RNN的前向传播相当于在每一个RNN单元内做前向传播工作,这里将前向传播的过程分为
        一个cell的前向传播和总体的前向传播
        now_time_stamps代表到第一个时间戳
        h_t1代表上一个时间戳的cell的输出
        x_t代表当前时间戳的词向量
    """

    def rnn_cell_forward(self, z_t):
        h_t = np.tanh(z_t)
        self.cache["h_t_list"].append(h_t)
        return h_t

    """
        x代表整个数据集
        times是一次前推过程中词向量矩阵的行数,也是步数的多少
    """

    def forward(self, x, times):
        h_t_shape = (self.output_size, 1)
        h_t = np.zeros((h_t_shape))
        # self.cache["y_hat_list"] = np.array();
        for t in range(times):
            # 得到一个cell的输出
            z_t = np.matmul(self.W, h_t) + np.matmul(self.U, x[t]) + self.bw
            h_t = self.rnn_cell_forward(z_t)
            o_t = self.V * h_t + self.bv
            # 通过一个softmax层
            y_hat = softmax(o_t)
            # 存储
            self.cache["x_t_list"].append(x[t])
            self.cache["z_t_list"].append(z_t)
            self.cache["y_hat_list"].append(y_hat)

        return
        pass

    # 计算多元交叉熵,这里必须保证y_hat和label的维度为categories*1,也就是有categories类
    def compute_cross_loss(self, y_hat, label, categories):
        loss = 0
        for i in range(categories):
            loss += label[i][0] * math.log2(y_hat[i][0])
        return -loss

    """
        反向传播部分
    """
    
    #代表求t时刻Lt关于o_t的导数
    def derivate_Lt_o_t(self,y_hat_t,y_t):
        return y_hat_t - y_t
    
    def derivate_Lt_v(self,derivate_Lt_o_t_value,h_t):
        return np.dot(derivate_Lt_o_t_value,np.transpose(h_t))

    def derivate_Lt_bv(self,derivate_Lt_o_t_value):
        return derivate_Lt_o_t_value

    # 为了计算方便设的中间变量
    def seita_t(self,z_t,v,derivate_Lt_o_t_value,w,is_limit,seita_t1):
        if(is_limit == 1):
            return np.dot(np.diag(1 - np.tanh(z_t) ** 2),np.dot(np.transpose(v),derivate_Lt_o_t_value))
        else:
            return np.dot(np.diag(1 - np.tanh(z_t) ** 2),np.dot(np.transpose(v),derivate_Lt_o_t_value) + np.dot(np.transpose(w),seita_t1))

    def derivate_Lt_w(self,seita_t_value,h_t):
        return np.dot(seita_t_value,np.transpose(h_t))

    def derivate_Lt_u(self,seita_t_value,x_t):
        return np.dot(seita_t_value,np.transpose(x_t))

    def derivate_Lt_bw(self,seita_t_value):
        return seita_t_value

    def update_weight(self,v_grad,bv_grad,w_grad,u_grad,bw_grad):
        self.V -= v_grad
        self.bv -= bv_grad
        self.W -= w_grad
        self.U -= u_grad
        self.bw -= bw_grad

    # 反向传播,计算并将梯度更新
    """
        times是总步数
        y是输入标签
    """
    def backward(self,times,y):
        v_grad = np.zeros((self.output_size,self.hidden_size))
        bv_grad = np.zeros((self.output_size,1))
        w_grad = np.zeros((self.hidden_size,self.hidden_size))
        u_grad = np.zeros((self.hidden_size,self.input_size))
        bw_grad = np.zeros((self.hidden_size,1))

        # 首先计算v_grad和bv_grad

        for t in range(times-1,-1,-1):
            derivate_Lt_o_t_value = self.derivate_Lt_o_t(self.cache["y_hat_list"][t],y)
            v_grad += self.lr * self.derivate_Lt_v(derivate_Lt_o_t_value,self.cache["h_t_list"][t])
            bv_grad += self.lr * self.derivate_Lt_bv(derivate_Lt_o_t_value)


        # 再计算w_grad,u_grad,bw_grad
        # 先计算最特殊的最后一步
        derivate_Lt_o_t_value = self.derivate_Lt_o_t(self.cache["y_hat_list"][times-1],y)
        seita_t1 = self.seita_t(self.cache["z_t_list"][times-1],self.V,derivate_Lt_o_t_value,1,-1)
        w_grad += self.lr * self.derivate_Lt_w(seita_t1,self.cache["h_t_list"][times-1])
        u_grad += self.lr * self.derivate_Lt_u(seita_t1,self.cache["x_t_list"][times-1])
        bw_grad += self.lr * self.derivate_Lt_bw(seita_t1)

        # 然后计算前面的times-1步
        for t in range(times -2,-1,-1):
            derivate_Lt_o_t_value = self.derivate_Lt_o_t(self.cache["y_hat_list"][t],y)
            seita_t1 = self.seita_t(self.cache["z_t_list"][t],self.V,derivate_Lt_o_t_value,0,seita_t1)
            w_grad += self.lr * self.derivate_Lt_w(seita_t1,self.cache["h_t_list"][t])
            u_grad += self.lr * self.derivate_Lt_u(seita_t1,self.cache["x_t_list"][t])
            bw_grad += self.lr * self.derivate_Lt_bw(seita_t1)
        pass

        # 更新权重
        self.update_weight(v_grad,bv_grad,w_grad,u_grad,bw_grad)



# h_t_shape = (2, 1)
# h_t = np.zeros((h_t_shape))
# print(h_t)


# h_t_shape =
# h_t = np.random.randn(3, 3)
# print(h_t)
# print(h_t[2][len(h_t[2]) -1 ])

# cache = {}
# # cache.t1 = [[1],[2]]
# cache["t1"] = [[1],[2]]
# print(cache["t1"])


# arrays = np.random.randn(1,5)
# print(arrays)
# arrays_1 = arrays - np.max(arrays)
# print(arrays_1)
# exp_x = np.exp(arrays - np.max(arrays))
# print(exp_x)
# np.

# x = _asarray_validated(x, check_finite=False)
# x_max = np.amax(x, axis=axis, keepdims=True)
# exp_x_shifted = np.exp(x - x_max)

# x = 12
# result = math.exp(x)
# print(result)


# import numpy as np

# # 创建一个二维数组
# array = np.array([[1, 2, 3]])

# # 转置数组
# transposed_array = np.transpose(array)

# # 或者使用数组的 T 属性
# # transposed_array = array.T

# print("原始数组：")
# print(array)
# print("\n转置后的数组：")
# print(transposed_array)


import numpy as np

# 假设 z_t 是一个向量
# z_t = np.array([1, -2, 3])

# 计算 1 - tanh(z_t)^2
# result = np.diag(1 - np.tanh(z_t)**2)


z_t = np.array([[1, 2, 3],[1, 2, 3],[1, 2, 3]])

# result = np.diag(z_t)

result = np.dot(z_t,z_t)
print(result)

result = z_t*z_t
print(result)

for i in range(3,1,-1):
    print(i)



h_t = np.zeros((3,4))
print(h_t)



z_t = np.array([[1, 2, 3],[1, 2, 3],[1, 2, 3]])
print(4*z_t)