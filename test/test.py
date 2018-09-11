# 2~4명의 주자로 이루어진 달리기 대회 랭킹을 보여주는 함수
def save_ranking(first, second, third=None, fourth=None):
    rank = {}
    rank[1], rank[2] = first, second
    rank[3] = third if third is not None else 'Nobody'
    rank[4] = fourth if fourth is not None else 'Nobody'
    print(rank)


# positional arguments 2개 전달
save_ranking('ming', 'alice')
# positional arguments 2개와 keyword argument 1개 전달
save_ranking('alice', 'ming', fourth='mike')
# positional arguments 2개와 keyword arguments 2개 전달 (단, 하나는 positional argument 형태로 전달)
save_ranking('alice', 'ming', 'mike', fourth='jim')

from functools import reduce

primes = [2, 3, 5, 7, 11, 13]
primes2 = {
    "1": "1",
    "2": "2",
    "3": "3"
}


def product(*numbers):
    print("product numbers : ", numbers)
    p = reduce(lambda x, y: x * y, numbers)
    return p


def product2(numbers):
    print("product2 numbers : ", numbers)
    numbers.append(1)


print(product(*primes))
# 30030

print(product(primes))
product2(primes)
print("after product2 : ", primes)


