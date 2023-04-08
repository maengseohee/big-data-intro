# [+] SparkConf, SparkContext 임포트
from pyspark import SparkConf, SparkContext
# [+] SparkConf, SparkContext 생성
conf = SparkConf().setMaster('local').setAppName('key-value-rdd')
sc = SparkContext(conf=conf)

# 파일 이름과 경로
path = './data/'
filename = 'rainshower.txt'
# [+] textFile()을 이용하여 원문(.txt)을 읽어 RDD 객체로 생성하기
lines = sc.textFile("./data/" + 'rainshower.txt')
# [+] take()을 이용하여 RDD 객체의 값 100개(= 100 줄) 출력
lines.take(100)

# [+] flatMap()를 이용하여 각 문장들을 단어 단위로 분리 및 저장
words = lines.flatMap(lambda x: x.split())
# [+] words 객체의 값 100개 출력
words.take(100)
# [+] map()을 이용하여 (단어, 1) 형태의 Key-Value RDD로 변환
word_pairs = lines.map(lambda x: (x, 1))
# [+] word_pairs 객체의 값 100개 출력
word_pairs.take(100)

# [+] reduceByKey()를 이용하여 단어별 빈도 세기
word_counts = word_pairs.reduceByKey(lambda a, b: a + b)
# sortBy()를 이용하여 빈도(x[1])를 기준으로 내림차순 정렬
sorted_word_counts = word_counts.sortBy(lambda x: x[1], ascending=False)
# [+] collect()를 이용하여 정렬된 단어별 빈도를 list 객체로 출력
res = word_pairs.collect()
# 상위 100개의 단어 및 빈도 출력
res[:100]
