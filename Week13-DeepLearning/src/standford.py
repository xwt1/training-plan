import stanza
import networkx as nx
import matplotlib.pyplot as plt

# 下载英语模型（可以替换为其他语言模型）
# stanza.download('en')

# 加载英语模型
nlp = stanza.Pipeline('en')

# 输入文本
text = "Mary eats an apple. She still feels hungry and wants to eat more apples. "

# 处理文本
doc = nlp(text)

# 在分词过程中，文本被切分成一个个离散的单元，
# 分词就是将连续的字序列按照一定的规范重新组合成词序列的过程。
# 我们知道，在英文的行文中，单词之间是以空格作为自然分界符的，
# 而中文只是字、句和段能通过明显的分界符来简单划界，唯独词没有一个形式上的分界符, 
# 分词过程就是找到这样分界符的过程.

# 分词
print("分词结果:")
for sentence in doc.sentences:
    for token in sentence.tokens:
        print(token.text)

# 顾名思义,区分每个分词的词性
# 词性标注
print("\n词性标注结果:")
for sentence in doc.sentences:
    for word in sentence.words:
        print(f"{word.text} : {word.upos}")

# 命名实体: 通常我们将人名, 地名, 机构名等专有名词统称命名实体. 如上句中Mary就是命名实体,
# 她被模型认为是人类,而如主语是电子科技大学就可能会被认为是地名

# 命名实体识别
print("\n命名实体识别结果:")
for sentence in doc.sentences:
    for ent in sentence.ents:
        print(f"{ent.text} : {ent.type}")

# 句法分析
# 比较常见的有两种:短语结构句法分析和依存句法分析

# 给我的感觉短语结构句法分析类似于编译原理的语法分析,
# 短语结构句法分析相当于通过特定语言的语法使用语法分析的方法构造语法树来发掘一句话中各个单词的关系
# 这颗语法树的叶子节点也就是终结符是单词,其余结点根据性质给予专门的叫法(如名词性短语,名词等),

# 如Marry eats an apple.这句话在短语结构语法分析中可能会长这个样子：
# S 代表句子（Sentence）
# NP 代表名词短语（Noun Phrase）
# VP 代表动词短语（Verb Phrase）
# Det 代表冠词（Determiner）
#          S
#      ____|____
#     NP        VP
#     |         |
#   Mary      eats
#               |
#            __|___
#           Det    NP
#            |      |
#           an    apple


# 而依存句法分析构造依存句法树。依存句法树表示了句子中单词和单词之间的依存关系
# 一个词语（通常是动词）被视为句子的核心，其他词语与核心词语存在依存关系


# 创建依存关系图
# 创建依存关系图
graph = {}

# 添加节点和边
for sentence in doc.sentences:
    for word in sentence.words:
        graph[word.id] = {'text': word.text, 'children': []}

for sentence in doc.sentences:
    for word in sentence.words:
        if word.head != 0:  # 0表示没有父节点
            graph[word.head]['children'].append(word.id)

# 打印依存关系树
def print_tree(node_id, level=0):
    if node_id in graph:
        node = graph[node_id]
        print("  " * level + f"{node['text']} ({node_id})")
        for child_id in node['children']:
            print_tree(child_id, level + 1)

# 从根节点开始打印
root_id = 1  # 这里假设根节点是1，可以根据实际情况调整
print_tree(root_id)
# 还是Marry eats an apple.
# 这时eats动词为核心，那么以eats为核心,Mary和apple有了关系
# 感觉这种分析方法更适合于英语那种动词为主的语言，对于汉语可能不适用?
#       eats
#    ____|____
#   |         |
# Mary      apple
#  |         |
#  |         an 


# for sentence in doc.sentences:
#     print("Sentence:", sentence.text)
#     for word in sentence.words:
#         print(f"Word: {word.text.ljust(15)}\tDeprel: {word.deprel.ljust(15)}\tHead: {word.head}")
#     print()

# 关闭 Stanza
# nlp.close()


