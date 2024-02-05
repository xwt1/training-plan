import math

import numpy as np


class BP_SGD():
    # 这里的IN,HN,ON分别为输入层，隐含层,输出层的维度，其中HN为一个一维数组，用来装对应下标层数的维度
    # layer_number为隐含层的层数，lr为学习率
    # 这里由于实验只要求一层，且节点数为10，我就偷懒不写成多层的了233，以后有改进可以数组多加一维
    # poch为训练次数
    def __init__(self, IN, ON, trainData, testData, layer_number=1, HN=10, lr=0.9, poch=10):
        np.random.seed(1)
        self.IN = IN
        self.HN = HN
        self.ON = ON
        self.layer_number = layer_number
        self.lr = lr
        self.thresholdHide = [0 for i in range(HN)]
        self.thresholdOut = [0 for i in range(ON)]
        self.poch = poch

        self.trainData = self.regulation(trainData)
        self.testData = self.regulation(testData)
        # self.W =[]    #这个地方可以写成三维的实现多层网络
        self.Win = [[0 for j in range(HN)] for i in range(IN)]
        self.Bin = [[0 for j in range(HN)] for i in range(IN)]
        self.Wout = [[0 for j in range(ON)] for i in range(HN)]
        self.Bout = [[0 for j in range(ON)] for i in range(HN)]
        for i in range(self.IN):
            for j in range(self.HN):
                self.Win[i][j] = np.random.randn() * 0.01
                self.Bin[i][j] = np.random.randn()
        for i in range(self.HN):
            for j in range(self.ON):
                self.Wout[i][j] = np.random.randn() * 0.01
                self.Bout[i][j] = np.random.randn()

    def regulation(self, Data):
        Data2 = [[0 for i in range(self.IN)] for j in range(len(Data))]
        for i in range(len(Data)):
            for j in range(self.IN):
                Data2[i][j] = Data[i][j]
        sum_row = [0 for i in range(self.IN)]
        for i in range(len(Data2)):
            for j in range(self.IN):
                sum_row[j] += Data2[i][j]
        for i in range(self.IN):
            sum_row[i] = sum_row[i] / len(Data2)
        IN_bias = [0 for i in range(self.IN)]
        for i in range(len(Data2)):
            for j in range(self.IN):
                Data2[i][j] -= sum_row[j]
                IN_bias[j] += math.pow(Data2[i][j], 2)
        for i in range(self.IN):
            IN_bias[i] = math.pow(IN_bias[i] / sum_row[i], 0.5)
        for i in range(len(Data)):
            for j in range(self.IN):
                Data[i][j] = (Data[i][j] - sum_row[j]) / IN_bias[j]
        return Data

    def sigmoids(self, x):
        return 1 / (1 + math.exp(x))

    def softmax(self, Data, index):
        sums = 0
        for i in range(len(Data)):
            sums += math.exp(Data[i])
        return math.exp(Data[index]) / sums

    def bp_forward(self, Nets, Label):
        nxt_Nets = []
        for j in range(self.HN):
            v = 0
            for i in range(self.IN):
                v += self.Win[i][j] * Nets[0][i] + self.Bin[i][j]
            nxt_Nets.append(self.sigmoids(-v))
        Nets.append(nxt_Nets)
        nxt_Nets = []
        for j in range(self.ON):
            v = 0
            for i in range(self.HN):
                v += self.Wout[i][j] * Nets[1][i] + self.Bout[i][j]
            nxt_Nets.append(self.sigmoids(-v))
        Nets.append(nxt_Nets)
        nxt_Nets2 = [0 for i in range(self.ON)]
        for i in range(len(nxt_Nets)):
            nxt_Nets2[i] = self.softmax(nxt_Nets, i)
        Nets.append(nxt_Nets2)
        Eloss = 0
        for i in range(self.ON):
            Eloss += Label[i] * math.log2(nxt_Nets[i])
        mx = -1
        mx_id = -1
        for i in range(self.ON):
            if mx < Nets[3][i]:
                mx = Nets[3][i]
                mx_id = i
        return mx_id, Eloss

    def bp_backward(self, Nets, Label):
        # 是损失函数对隐含层的输入值求导结果
        loss_h = []
        for i in range(self.ON):
            loss_h.append(Nets[3][i] - Label[i])
        #
        loss_h2 = [0 for i in range(self.HN)]
        for i in range(self.ON):
            loss_h2[i] = loss_h[i] * Nets[2][i] * (1 - Nets[2][i])

        #
        loss_h3 = [0 for i in range(self.HN)]
        for i in range(self.HN):
            v = 0
            for j in range(self.ON):
                v += self.Wout[i][j] * loss_h2[j]
            loss_h3[i] = v
        #
        loss_h4 = [0 for i in range(self.HN)]
        for i in range(self.HN):
            loss_h4[i] = Nets[1][i] * (1 - Nets[1][i]) * loss_h3[i]
        #
        for i in range(self.HN):
            for j in range(self.ON):
                self.Wout[i][j] -= self.lr * Nets[1][i] * loss_h2[j]
        #
        for i in range(self.HN):
            for j in range(self.ON):
                self.Bout[i][j] -= self.lr * loss_h2[j]
        for i in range(self.IN):
            for j in range(self.HN):
                self.Win[i][j] -= self.lr * Nets[0][i] * loss_h4[j]
        for i in range(self.IN):
            for j in range(self.HN):
                self.Bin[i][j] -= self.lr * loss_h4[j]

    def train(self):
        # for i in range(self.poch):
        for j in range(len(self.trainData)):
            # nets是神经网路中给节点值输出的矩阵
            Nets = [[self.trainData[j][temp] for temp in range(len(self.trainData[j])) if temp < self.IN]]
            Label = [0 for i in range(self.ON)]
            Label[int(self.trainData[j][self.IN])] = 1
            self.bp_forward(Nets, Label)
            self.bp_backward(Nets, Label)

    def predict(self, epochth):
        cnt = 0
        for i in range(len(self.testData)):
            test_out = [[self.testData[i][j] for j in range(len(self.testData[i])) if j < self.IN]]
            Label = [0 for i in range(self.ON)]
            Label[int(self.testData[i][self.IN])] = 1
            results = self.bp_forward(test_out, Label)[0]
            if results == self.testData[i][self.IN]:
                cnt += 1
            # print("预测：{0}".format(results), "实际：{0}".format(self.testData[i][self.IN]))
        print("第{0}次：".format(epochth), "准确率：{0}".format(cnt / len(self.testData)))
        return cnt / len(self.testData)


def read_txt(dire):
    Data_float = []
    with open(dire, 'r') as f:
        my_data = f.readlines()  # txt中所有字符串读入data，得到的是一个list
        # 对list中的数据做分隔和类型转换
        for line in my_data:
            line = line.strip('\n')
            line_data = line.split()
            if len(line_data) == 1:
                continue
            Temp_Float = list(map(float, line_data))  # 转化为浮点数
            # if len(Data_float) == 0:
            #     TempS = [i for i in range(1, len(line_data) + 1)]
            #     Data_float.append(TempS)
            Data_float.append(Temp_Float)

    return Data_float


def main():
    test_data = read_txt("Iris-test.txt")
    train_data = read_txt("Iris-train.txt")
    bp = BP_SGD(4, 3, train_data, test_data)
    pred = []
    avg = 0
    epochs = 1000
    for ee in range(10):
        for i in range(epochs):
            bp.train()
            if i == epochs-1:
                thiss = bp.predict(ee)
                pred.append(thiss)
                avg += thiss
    avg /= len(pred)
    seita = 0
    for i in range(len(pred)):
        seita += math.pow(pred[i] - avg, 2)
    seita = math.sqrt(seita / len(pred))
    print("独立十次的平均值：{0}".format(avg), "标准差：{0}".format(seita))


if __name__ == '__main__':
    main()
